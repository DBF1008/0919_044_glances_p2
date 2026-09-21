#
# This file is part of Glances.
#
# SPDX-FileCopyrightText: 2024 Nicolas Hennion <nicolas@nicolargo.com>
#
# SPDX-License-Identifier: LGPL-3.0-only
#

"""Generic realtime stats over WebSocket.

This module provides a server-push alternative to the RESTful API polling:
clients connect to the ``/ws/stats`` endpoint, subscribe to a comma separated
list of plugins (or ``*`` for every enabled plugin) and receive a message
every time a plugin finishes its own refresh cycle.

Backpressure is controlled by the client: each stats message carries a
sequence number and must be acknowledged. While a plugin has one or more
unacknowledged messages, pushes for that plugin are paused (new values are
coalesced) without affecting the other plugins subscribed by the same
connection or the other connections.
"""

import asyncio
import itertools
import time

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from glances.globals import json_dumps, json_loads
from glances.logger import logger

# Protocol version sent in the hello message
WS_PROTOCOL_VERSION = 1

# WebSocket close codes (https://www.rfc-editor.org/rfc/rfc6455#section-7.4)
WS_CLOSE_POLICY_VIOLATION = 1008
WS_CLOSE_TRY_AGAIN_LATER = 1013


class GlancesWebSocketConnection:
    """A single WebSocket connection and its subscription state."""

    def __init__(self, websocket: WebSocket, connection_id: int):
        self.websocket = websocket
        self.connection_id = connection_id

        # Subscribed plugin names. An empty set means "every plugin" (*).
        self.subscribe_all = True
        self.subscriptions = set()

        # Per plugin sequence number, pending (unacknowledged) plugin names,
        # latest payload coalesced while paused and dropped message counter.
        self._sequences = {}
        self._seq_counter = itertools.count(1)
        self._pending = set()
        self._pending_data = {}
        self._dropped = {}

        # Last payload already sent for a plugin, used to avoid sending
        # identical values twice in a row.
        self._last_sent = {}

        # One writer at a time on the socket.
        self.send_lock = asyncio.Lock()

        # Idle timeout management
        self.last_activity = time.monotonic()
        self.ping_sent = False

    def touch(self):
        """Record client activity (any received frame)."""
        self.last_activity = time.monotonic()
        self.ping_sent = False

    def is_subscribed(self, plugin_name: str) -> bool:
        """Return True if the connection wants stats for the given plugin."""
        return self.subscribe_all or plugin_name in self.subscriptions

    def set_subscriptions(self, plugins):
        """Set the subscription list.

        ``plugins`` is a list of plugin names; an empty list subscribes to
        every plugin.
        """
        if plugins:
            self.subscribe_all = False
            self.subscriptions = set(plugins)
        else:
            self.subscribe_all = True
            self.subscriptions = set()
        # Reset backpressure state for plugins no longer subscribed
        for plugin_name in list(self._pending):
            if not self.is_subscribed(plugin_name):
                self._pending.discard(plugin_name)
                self._pending_data.pop(plugin_name, None)
                self._dropped.pop(plugin_name, None)

    def offer(self, plugin_name: str, data):
        """Offer a fresh plugin payload.

        Return True if the connection is ready to send it, False if the push
        for this plugin must be paused (an older message is unacknowledged).
        When paused the latest value is coalesced: intermediate values are
        dropped and will be accounted in the next pushed message.
        """
        if not self.is_subscribed(plugin_name):
            return False
        if data == self._last_sent.get(plugin_name):
            # The plugin refreshed but the value did not change
            return False
        if plugin_name in self._pending:
            # Backpressure: pause this plugin, keep only the latest value
            if plugin_name in self._pending_data:
                self._dropped[plugin_name] = self._dropped.get(plugin_name, 0) + 1
            self._pending_data[plugin_name] = data
            return False
        return True

    def _next_sequence(self, plugin_name: str) -> int:
        return next(self._seq_counter)

    def build_stats_message(self, plugin_name: str, data, sequence=None):
        """Build the stats message envelope for the given payload."""
        if sequence is None:
            sequence = self._next_sequence(plugin_name)
        self._sequences[plugin_name] = sequence
        self._last_sent[plugin_name] = data
        message = {
            'type': 'stats',
            'plugin': plugin_name,
            'seq': sequence,
            'data': data,
        }
        dropped = self._dropped.pop(plugin_name, 0)
        if dropped:
            message['dropped'] = dropped
        return message

    def mark_pending(self, plugin_name: str):
        """Mark the last sent message of a plugin as unacknowledged."""
        self._pending.add(plugin_name)

    async def send_json(self, message: dict):
        """Serialize and send a JSON text frame (one writer at a time)."""
        async with self.send_lock:
            await self.websocket.send_text(json_dumps(message).decode('utf-8'))

    async def send_stats(self, plugin_name: str, data):
        """Send a stats message and mark the plugin as awaiting an ACK."""
        message = self.build_stats_message(plugin_name, data)
        await self.send_json(message)
        self.mark_pending(plugin_name)

    async def send_error(self, message_id, error_message: str):
        """Send an error frame back to the client."""
        await self.send_json({'type': 'error', 'id': message_id, 'error': error_message})

    def ack(self, plugin_name: str, sequence=None):
        """Acknowledge a stats message.

        Return a ``(data, sequence)`` tuple if a newer value was coalesced
        while the plugin was paused (it must be pushed immediately), or
        ``(None, None)`` if there is nothing pending.
        """
        if plugin_name not in self._pending:
            return None, None
        self._pending.discard(plugin_name)
        pending_data = self._pending_data.pop(plugin_name, None)
        if pending_data is None or pending_data == self._last_sent.get(plugin_name):
            return None, None
        # The latest value changed during the pause: build the next message.
        return self.build_stats_message(plugin_name, pending_data), None

    def pending_plugins(self):
        """Return the set of plugins with an unacknowledged message."""
        return set(self._pending)


class GlancesWebSocketManager:
    """Manage WebSocket connections and broadcast plugin stats.

    Features:
    - maximum number of connections (rate limiting)
    - idle timeout (clients not receiving/answering are disconnected)
    - per-connection subscription filtering (plugin list or ``*``)
    - per-connection, per-plugin backpressure driven by client ACKs
    """

    def __init__(
        self,
        stats=None,
        max_connections=100,
        idle_timeout=60,
        url_prefix='',
        authenticator=None,
    ):
        # Stats instance is set at server start (was None at construction)
        self.stats = stats
        self.max_connections = max(1, int(max_connections))
        self.idle_timeout = float(idle_timeout)
        self.url_prefix = url_prefix or ''
        # Optional async callable(WebSocket) -> username or None
        self.authenticator = authenticator

        self._connections = set()
        self._connections_lock = asyncio.Lock()
        self._connection_ids = itertools.count(1)

        # Event loop the stats thread talks to
        self._loop = None

    def set_stats(self, stats):
        """Bind the GlancesStats instance and register as observer."""
        self.stats = stats
        if stats is not None:
            stats.add_observer(self.on_plugin_updated)

    def router(self) -> APIRouter:
        """Build the APIRouter exposing the WebSocket endpoint."""
        router = APIRouter(prefix=self.url_prefix)
        router.add_api_websocket_route('/ws/stats', self._ws_endpoint, name='ws_stats')
        return router

    # ------------------------------------------------------------------ #
    # Observer side: called from the stats update thread
    # ------------------------------------------------------------------ #

    def on_plugin_updated(self, plugin_name, plugin):
        """Stats observer callback: schedule the broadcast on the event loop."""
        loop = self._loop
        if loop is None or not loop.is_running():
            return
        try:
            data = plugin.get_api()
        except Exception as e:
            logger.error(f"WebSocket: cannot get stats for plugin {plugin_name}: {e}")
            return
        # call_soon_threadsafe is a cheap thread handoff; the coroutine does
        # the per-connection filtering/serialization on the event loop thread.
        loop.call_soon_threadsafe(asyncio.ensure_future, self._broadcast(plugin_name, data))

    async def _broadcast(self, plugin_name: str, data):
        """Offer a fresh plugin payload to every subscribed connection."""
        async with self._connections_lock:
            connections = list(self._connections)
        tasks = []
        for connection in connections:
            try:
                ready = connection.offer(plugin_name, data)
            except Exception:
                ready = False
            if ready:
                tasks.append(self._send_to_connection(connection, plugin_name, data))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_to_connection(self, connection: GlancesWebSocketConnection, plugin_name: str, data):
        """Send a payload and handle slow/broken consumers."""
        try:
            await connection.send_stats(plugin_name, data)
        except Exception as e:
            logger.debug(f"WebSocket: send failed for connection {connection.connection_id}: {e}")
            await self._unregister(connection)

    # ------------------------------------------------------------------ #
    # Connection lifecycle
    # ------------------------------------------------------------------ #

    async def _register(self, connection: GlancesWebSocketConnection) -> bool:
        """Add a connection. Return False if the limit is reached."""
        async with self._connections_lock:
            if len(self._connections) >= self.max_connections:
                return False
            self._connections.add(connection)
            return True

    async def _unregister(self, connection: GlancesWebSocketConnection):
        """Remove a connection and close its socket best effort."""
        async with self._connections_lock:
            self._connections.discard(connection)
        try:
            await connection.websocket.close()
        except Exception:
            pass

    def connection_count(self) -> int:
        """Return the current number of live connections."""
        return len(self._connections)

    async def close_all(self):
        """Close every connection (server shutdown)."""
        async with self._connections_lock:
            connections = list(self._connections)
            self._connections.clear()
        for connection in connections:
            try:
                await connection.websocket.close()
            except Exception:
                pass

    # ------------------------------------------------------------------ #
    # WebSocket endpoint
    # ------------------------------------------------------------------ #

    def _parse_subscriptions(self, raw):
        """Parse a comma separated plugin list.

        Return ``(plugins, invalid)``: ``plugins`` is the list of valid,
        enabled plugin names (an empty list means ``*``) and ``invalid`` the
        list of unknown plugin names.
        """
        if raw is None:
            raw = '*'
        raw = raw.strip()
        if raw == '' or raw == '*':
            return [], []
        available = set(self.stats.getPluginsList())
        plugins = []
        invalid = []
        for name in (item.strip() for item in raw.split(',')):
            if not name:
                continue
            if name in available and name not in plugins:
                plugins.append(name)
            elif name not in available:
                invalid.append(name)
        return plugins, invalid

    async def _ws_endpoint(self, websocket: WebSocket):
        """Handle a /ws/stats connection.

        Query parameters:
        - plugins: comma separated plugin names or ``*`` (default ``*``)
        - token: JWT bearer token, used by browsers unable to set the
          Authorization header on a WebSocket handshake
        """
        # Remember the event loop so the stats thread can talk to us
        self._loop = asyncio.get_running_loop()

        # Authentication happens before accepting the handshake.
        if self.authenticator is not None:
            try:
                authenticated = await self.authenticator(websocket)
            except Exception:
                authenticated = False
            if not authenticated:
                # Close before accept: the server replies with an HTTP 403.
                await websocket.close(code=WS_CLOSE_POLICY_VIOLATION)
                return

        plugins_param = websocket.query_params.get('plugins', '*')
        plugins, invalid = self._parse_subscriptions(plugins_param)

        connection = GlancesWebSocketConnection(websocket, next(self._connection_ids))
        if not await self._register(connection):
            await websocket.close(
                code=WS_CLOSE_TRY_AGAIN_LATER,
                reason='Too many WebSocket connections',
            )
            return

        await websocket.accept()
        connection.set_subscriptions(plugins)
        logger.info(
            f"WebSocket client {connection.connection_id} connected "
            f"({self.connection_count()}/{self.max_connections}), "
            f"subscriptions: {'*' if connection.subscribe_all else ','.join(sorted(connection.subscriptions))}"
        )

        await connection.send_json(
            {
                'type': 'hello',
                'protocol': WS_PROTOCOL_VERSION,
                'connection_id': connection.connection_id,
                'subscriptions': ['*'] if connection.subscribe_all else sorted(connection.subscriptions),
                'plugins': self.stats.getPluginsList(),
            }
        )
        if invalid:
            await connection.send_error(None, f"Unknown plugin(s): {', '.join(invalid)}")

        try:
            await self._receive_loop(connection)
        except WebSocketDisconnect:
            logger.debug(f"WebSocket client {connection.connection_id} disconnected")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.debug(f"WebSocket client {connection.connection_id} closed with error: {e}")
        finally:
            await self._unregister(connection)
            logger.info(
                f"WebSocket client {connection.connection_id} removed "
                f"({self.connection_count()}/{self.max_connections} remaining)"
            )

    async def _receive_loop(self, connection: GlancesWebSocketConnection):
        """Read and dispatch client messages, enforcing the idle timeout."""
        websocket = connection.websocket
        while True:
            try:
                incoming = await asyncio.wait_for(websocket.receive(), timeout=self.idle_timeout / 2.0)
            except asyncio.TimeoutError:
                # Half of the idle timeout is used to send an application
                # level ping; the client gets the remaining half to answer.
                if not connection.ping_sent:
                    connection.ping_sent = True
                    try:
                        await connection.send_json({'type': 'ping'})
                    except Exception:
                        return
                    continue
                # No activity at all during the full idle timeout.
                logger.info(
                    f"WebSocket client {connection.connection_id} disconnected "
                    f"after {self.idle_timeout} seconds idle"
                )
                try:
                    await websocket.close(code=1001, reason='Idle timeout')
                except Exception:
                    pass
                return

            if incoming.get('type') == 'websocket.disconnect':
                return
            if incoming.get('type') != 'websocket.receive':
                continue
            connection.touch()

            if incoming.get('bytes') is not None:
                # Binary frames are not part of the protocol
                await connection.send_error(None, 'Binary frames are not supported')
                continue

            text = incoming.get('text')
            if text is None:
                continue
            try:
                message = json_loads(text)
            except Exception:
                await connection.send_error(None, 'Invalid JSON message')
                continue
            if not isinstance(message, dict):
                await connection.send_error(None, 'Message must be a JSON object')
                continue
            await self._handle_message(connection, message)

    async def _handle_message(self, connection: GlancesWebSocketConnection, message: dict):
        """Dispatch a single client message."""
        message_type = message.get('type')
        message_id = message.get('id')

        if message_type in ('ping', 'pong'):
            await connection.send_json({'type': 'pong'})
            return

        if message_type == 'subscribe':
            plugins, invalid = self._parse_subscriptions(message.get('plugins', '*'))
            connection.set_subscriptions(plugins)
            await connection.send_json(
                {
                    'type': 'subscribed',
                    'id': message_id,
                    'subscriptions': ['*'] if connection.subscribe_all else sorted(connection.subscriptions),
                }
            )
            if invalid:
                await connection.send_error(message_id, f"Unknown plugin(s): {', '.join(invalid)}")
            return

        if message_type == 'ack':
            plugin_name = message.get('plugin')
            if not plugin_name:
                await connection.send_error(message_id, 'Missing plugin in ack message')
                return
            next_message, _ = connection.ack(plugin_name, message.get('seq'))
            if next_message is not None:
                # A fresher value was coalesced while the plugin was paused:
                # send it immediately and mark the plugin pending again.
                next_message['id'] = message_id
                try:
                    await connection.send_json(next_message)
                    connection.mark_pending(plugin_name)
                except Exception:
                    await self._unregister(connection)
            return

        await connection.send_error(message_id, f"Unknown message type: {message_type}")
