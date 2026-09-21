#
# This file is part of Glances.
#
# SPDX-FileCopyrightText: 2024 Nicolas Hennion <nicolas@nicolargo.com>
#
# SPDX-License-Identifier: LGPL-3.0-only
#

"""WebSocket push interface for Glances stats.

This module implements a generic real-time push channel on top of the
FastAPI/uvicorn stack (endpoint: /ws/stats).

Features:
- Per-connection subscription filter: clients subscribe to a comma-separated
  list of plugins (?plugins=cpu,mem) or to all of them (?plugins=*).
- Server-side scheduling: subscribed plugins are updated at their own refresh
  rate (plugin.get_refresh()); the WebSocket layer never polls the stats
  object, it is notified through the GlancesStats observer mechanism
  (see GlancesStats.register_update_observer in glances/stats.py).
- Backpressure: every 'stats' message carries a sequence number and must be
  confirmed by the client with an 'ack' message. While a plugin message is
  unacknowledged, new messages for this plugin are paused for this connection
  only; other plugins and other connections are not blocked.
- Connection management: maximum number of simultaneous connections and
  idle timeout (a connection with no inbound message is closed).

Protocol (JSON text frames):
- Server -> client: {"type": "subscribed", "plugins": [...], "unknown_plugins": [...]}
- Server -> client: {"type": "stats", "plugin": "cpu", "seq": 1, "ts": 0.0, "data": {...}}
- Server -> client: {"type": "pong", "ts": 0.0}
- Client -> server: {"type": "ack", "plugin": "cpu", "seq": 1}
- Client -> server: {"type": "ping"}
"""

import asyncio
import json
import threading
import time

from glances.globals import json_dumps
from glances.logger import logger

# Subscription keyword matching every enabled plugin
ALL_PLUGINS = '*'

# How often (seconds) the scheduler checks if a subscribed plugin is due
# for an update (the plugin refresh rate itself is enforced per plugin)
SCHEDULER_INTERVAL = 0.5


class WebSocketClientSession:
    """State attached to one connected WebSocket client."""

    def __init__(self, websocket, plugins):
        # The starlette WebSocket instance
        self.websocket = websocket
        # Set of subscribed plugin names, or {ALL_PLUGINS}
        self.plugins = plugins
        # Backpressure state: plugin name -> sequence number awaiting client ack
        self.pending_ack = {}
        # Monotonic timestamp of the last inbound message (idle detection)
        self.last_activity = time.monotonic()
        # Sequence number generator (per connection)
        self.seq = 0

    def is_subscribed(self, plugin_name):
        """Return True if the session subscribed to the given plugin."""
        return ALL_PLUGINS in self.plugins or plugin_name in self.plugins

    def is_blocked(self, plugin_name):
        """Return True if an unacknowledged message is pending for this plugin."""
        return plugin_name in self.pending_ack

    def next_seq(self):
        """Return the next message sequence number for this connection."""
        self.seq += 1
        return self.seq


class GlancesWebSocketManager:
    """Manage WebSocket connections pushing Glances stats to clients."""

    def __init__(self, args=None, config=None, max_connections=16, idle_timeout=300):
        self.args = args
        self.config = config
        # Maximum number of simultaneous WebSocket connections
        self.max_connections = max_connections
        # Close connections with no inbound message after this delay (seconds)
        self.idle_timeout = idle_timeout
        # GlancesStats instance (set with set_stats)
        self._stats = None
        # Active WebSocketClientSession instances
        self._sessions = set()
        # Protect _sessions: mutated from the asyncio loop and read from
        # the stats update thread (observer callback)
        self._lock = threading.Lock()
        # asyncio loop of the uvicorn server (captured on first connection)
        self._loop = None
        # Background task triggering plugin updates at their refresh rate
        self._scheduler_task = None
        # Last time (monotonic) an update was scheduled for a plugin
        self._last_update_trigger = {}

    def set_stats(self, stats):
        """Set the GlancesStats instance used as data source."""
        self._stats = stats

    @property
    def connections_count(self):
        """Return the number of active WebSocket connections."""
        with self._lock:
            return len(self._sessions)

    def _enabled_plugins(self):
        """Return the list of enabled plugin names (empty if no stats)."""
        if self._stats is None:
            return []
        return self._stats.getPluginsList()

    def _parse_subscription(self, plugins_param):
        """Parse the subscription query parameter.

        Return a (plugins, unknown) tuple: plugins is {ALL_PLUGINS} or a set
        of valid plugin names, unknown is the list of requested but unknown
        plugin names.
        """
        if plugins_param is None or plugins_param.strip() in ('', ALL_PLUGINS):
            return {ALL_PLUGINS}, []
        enabled = set(self._enabled_plugins())
        requested = [p.strip() for p in plugins_param.split(',') if p.strip()]
        known = {p for p in requested if p in enabled}
        unknown = [p for p in requested if p not in enabled]
        return known, unknown

    def _subscribed_plugins(self):
        """Return the union of the plugins subscribed by all the sessions."""
        with self._lock:
            subscriptions = [s.plugins for s in self._sessions]
        if not subscriptions:
            return []
        if any(ALL_PLUGINS in s for s in subscriptions):
            return self._enabled_plugins()
        return sorted(set().union(*subscriptions))

    def _has_subscriber(self, plugin_name):
        """Return True if at least one session subscribes to the plugin."""
        with self._lock:
            return any(s.is_subscribed(plugin_name) for s in self._sessions)

    # ------------------------------------------------------------------
    # Observer entry point (called by GlancesStats from the update thread)
    # ------------------------------------------------------------------

    def notify_plugin_updated(self, plugin_name):
        """Observer callback triggered by GlancesStats after a plugin update.

        Called from the thread performing the stats update: it must be fast
        and non-blocking, the broadcast itself is scheduled on the asyncio
        loop of the uvicorn server.
        """
        loop = self._loop
        if loop is None or self._stats is None:
            return
        if not self._has_subscriber(plugin_name):
            return
        try:
            asyncio.run_coroutine_threadsafe(self._broadcast(plugin_name), loop)
        except RuntimeError as e:
            # Event loop is closed (server shutting down)
            logger.debug(f'WebSocket broadcast skipped for {plugin_name}: {e}')

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def handle_connection(self, websocket, plugins_param=None):
        """Manage the full lifecycle of one WebSocket connection."""
        # Connection rate limiting: refuse when the maximum is reached
        if self.connections_count >= self.max_connections:
            await websocket.accept()
            await websocket.close(code=1013, reason='Too many connections')
            logger.debug('WebSocket connection refused: maximum connections reached')
            return

        await websocket.accept()
        # Capture the asyncio loop for the thread-safe observer callback
        self._loop = asyncio.get_running_loop()

        plugins, unknown = self._parse_subscription(plugins_param)
        session = WebSocketClientSession(websocket, plugins)
        with self._lock:
            self._sessions.add(session)
        self._ensure_scheduler()
        logger.debug(f'WebSocket client connected (subscriptions: {sorted(plugins)})')

        try:
            await self._send(
                session,
                {
                    'type': 'subscribed',
                    'plugins': sorted(plugins),
                    'unknown_plugins': unknown,
                    'idle_timeout': self.idle_timeout,
                },
            )
            # Push the current stats snapshot for the subscribed plugins
            for plugin_name in self._enabled_plugins():
                if session.is_subscribed(plugin_name):
                    await self._send_stats(session, plugin_name)
            await self._receive_loop(session)
        finally:
            self._discard_session(session)

    async def _receive_loop(self, session):
        """Process inbound messages (ack, ping) until disconnect or idle timeout."""
        websocket = session.websocket
        while True:
            try:
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=self.idle_timeout)
            except asyncio.TimeoutError:
                logger.debug('WebSocket connection closed: idle timeout')
                await websocket.close(code=1001, reason='Idle timeout')
                break
            except Exception:
                # Client disconnected (starlette raises WebSocketDisconnect)
                break

            session.last_activity = time.monotonic()

            try:
                message = json.loads(raw)
            except ValueError:
                await self._send(session, {'type': 'error', 'message': 'Invalid JSON message'})
                continue

            message_type = message.get('type')
            if message_type == 'ack':
                self._handle_ack(session, message.get('plugin'), message.get('seq'))
            elif message_type == 'ping':
                await self._send(session, {'type': 'pong', 'ts': time.time()})
            else:
                await self._send(session, {'type': 'error', 'message': f'Unknown message type: {message_type}'})

    def _handle_ack(self, session, plugin_name, seq):
        """Process a client acknowledgement (backpressure release)."""
        pending = session.pending_ack.get(plugin_name)
        if pending is None:
            return
        # Accept exact or cumulative (newer) sequence numbers
        if isinstance(seq, int) and seq >= pending:
            del session.pending_ack[plugin_name]

    def _discard_session(self, session):
        """Remove a session from the active connections."""
        with self._lock:
            self._sessions.discard(session)
        logger.debug(f'WebSocket client disconnected ({self.connections_count} remaining)')

    # ------------------------------------------------------------------
    # Outbound messages
    # ------------------------------------------------------------------

    async def _send(self, session, message):
        """Send a JSON message to one session (Glances JSON serialization)."""
        await session.websocket.send_text(json_dumps(message).decode('utf-8'))

    async def _send_stats(self, session, plugin_name):
        """Send the current stats of one plugin to one session (with backpressure)."""
        if session.is_blocked(plugin_name):
            # Previous message not acknowledged yet: pause this plugin
            # for this connection (other plugins/connections are unaffected)
            return False
        plugin = self._stats.get_plugin(plugin_name)
        if plugin is None:
            return False
        seq = session.next_seq()
        await self._send(
            session,
            {
                'type': 'stats',
                'plugin': plugin_name,
                'seq': seq,
                'ts': time.time(),
                'data': plugin.get_raw(),
            },
        )
        session.pending_ack[plugin_name] = seq
        return True

    async def _broadcast(self, plugin_name):
        """Push fresh stats of the given plugin to all eligible sessions."""
        with self._lock:
            targets = [s for s in self._sessions if s.is_subscribed(plugin_name)]
        for session in targets:
            try:
                await self._send_stats(session, plugin_name)
            except Exception as e:
                logger.debug(f'WebSocket send failed for {plugin_name}: {e}')
                self._discard_session(session)

    # ------------------------------------------------------------------
    # Update scheduler (per-plugin refresh rate)
    # ------------------------------------------------------------------

    def _ensure_scheduler(self):
        """Start the background update scheduler if not already running."""
        if self._scheduler_task is None or self._scheduler_task.done():
            self._scheduler_task = asyncio.create_task(self._scheduler_loop())

    def _plugin_refresh(self, plugin_name):
        """Return the refresh interval (seconds) of a plugin (minimum 1s)."""
        try:
            plugin = self._stats.get_plugin(plugin_name)
            refresh = plugin.get_refresh() if plugin is not None else None
        except Exception:
            refresh = None
        if not refresh or refresh < 1:
            refresh = 1
        return refresh

    async def _scheduler_loop(self):
        """Trigger stats updates for subscribed plugins at their refresh rate."""
        while True:
            await asyncio.sleep(SCHEDULER_INTERVAL)
            if self._stats is None or self.connections_count == 0:
                continue
            now = time.monotonic()
            for plugin_name in self._subscribed_plugins():
                last_trigger = self._last_update_trigger.get(plugin_name, 0)
                if now - last_trigger < self._plugin_refresh(plugin_name):
                    continue
                self._last_update_trigger[plugin_name] = now
                try:
                    # Run the (blocking) update outside of the event loop;
                    # on completion GlancesStats notifies the observers which
                    # schedule the broadcast back on this loop
                    await asyncio.to_thread(self._stats.update, plugins_list_to_update=[plugin_name])
                except Exception as e:
                    logger.debug(f'WebSocket scheduler update failed for {plugin_name}: {e}')

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def stop(self):
        """Stop the scheduler and close all the connections (any thread)."""
        loop = self._loop
        if loop is not None:
            try:
                asyncio.run_coroutine_threadsafe(self._shutdown(), loop)
            except RuntimeError:
                pass

    async def _shutdown(self):
        """Cancel the scheduler and close every active session."""
        if self._scheduler_task is not None:
            self._scheduler_task.cancel()
            self._scheduler_task = None
        with self._lock:
            sessions = list(self._sessions)
        for session in sessions:
            try:
                await session.websocket.close(code=1001)
            except Exception:
                pass
            self._discard_session(session)
