import asyncio
from typing import TYPE_CHECKING, Any, Dict, List

from hummingbot.connector.exchange.chainring import chainring_constants as CONSTANTS, chainring_web_utils as web_utils
from hummingbot.connector.exchange.chainring.chainring_auth import ChainringAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.chainring.chainring_exchange import ChainringExchange


class ChainringAPIUserStreamDataSource(UserStreamTrackerDataSource):

    def __init__(self,
                 auth: ChainringAuth,
                 trading_pairs: List[str],
                 connector: 'ChainringExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth: ChainringAuth = auth
        self._connector = connector
        self._domain = domain
        self._api_factory = api_factory

        self._listen_key_initialized_event: asyncio.Event = asyncio.Event()
        self._last_listen_key_ping_ts = 0

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange
        """
        ws: WSAssistant = await self._get_ws_assistant()
        url = f"{web_utils.wss_url(self._auth.sign_login_action(), self._domain)}"
        await ws.connect(ws_url=url, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param websocket_assistant: the websocket assistant used to connect to the exchange
        """
        try:
            subscribe_balances_payload = await self.subscribe_payload("Balances")
            subscribe_limits_payload = await self.subscribe_payload("Limits")
            subscribe_orders_payload = await self.subscribe_payload("MyOrders")
            subscribe_trades_payload = await self.subscribe_payload("MyTrades")

            await websocket_assistant.send(WSJSONRequest(payload=subscribe_balances_payload))
            await websocket_assistant.send(WSJSONRequest(payload=subscribe_limits_payload))
            await websocket_assistant.send(WSJSONRequest(payload=subscribe_orders_payload))
            await websocket_assistant.send(WSJSONRequest(payload=subscribe_trades_payload))

            self.logger().info("Subscribed to user streams...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to user streams...",
                exc_info=True
            )
            raise

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        if (
                len(event_message) > 0
                and "topic" in event_message
                and "type" in event_message.get("topic")
                and event_message.get("topic").get("type") in ["Balances", "Limits", "MyOrders", "MyTrades"]
        ):
            queue.put_nowait(event_message)

    @staticmethod
    async def subscribe_payload(channel):
        payload = {
            "type": "Subscribe",
            "topic": {
                "type": channel,
            },
        }
        return payload
