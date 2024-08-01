import asyncio
import time
import urllib.parse
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.chainring import chainring_constants as CONSTANTS, chainring_web_utils as web_utils
from hummingbot.connector.exchange.chainring.chainring_auth import ChainringAuth
from hummingbot.connector.exchange.chainring.chainring_order_book import ChainringOrderBook
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.chainring.chainring_exchange import ChainringExchange


class ChainringAPIOrderBookDataSource(OrderBookTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: ChainringAuth,
                 trading_pairs: List[str],
                 connector: 'ChainringExchange',
                 api_factory: WebAssistantsFactory,
                 time_synchronizer: TimeSynchronizer,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._auth: ChainringAuth = auth
        self._connector = connector
        self._domain = domain
        self._api_factory = api_factory
        self._time_synchronizer = time_synchronizer

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.
        :param trading_pair: the trading pair for which the order book will be retrieved
        :return: the response from the exchange (JSON dictionary)
        """
        market_id = await self._connector.exchange_symbol_associated_to_pair(trading_pair)
        escaped_market_id = urllib.parse.quote_plus(market_id, safe='')

        rest_assistant = await self._api_factory.get_rest_assistant()
        snapshot = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.ORDER_BOOK_PATH_URL.format(escaped_market_id), domain=self._domain),
            is_auth_required=True,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.ALL_HTTP,
        )

        order_book_message: OrderBookMessage = ChainringOrderBook.snapshot_message_from_exchange(
            msg=snapshot, timestamp=self._time_synchronizer.time(), metadata={"trading_pair": trading_pair}
        )

        return order_book_message

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            for trading_pair in self._trading_pairs:
                market_id = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                await ws.send(WSJSONRequest(payload={
                    "type": "Subscribe",
                    "topic": {
                        "type": "OrderBook",
                        "marketId": market_id
                    },
                }))
                await ws.send(WSJSONRequest(payload={
                    "type": "Subscribe",
                    "topic": {
                        "type": "MarketTrades",
                        "marketId": market_id
                    },
                }))

            self.logger().info(f"Subscribed to order book and trade channels for: {', '.join(self._trading_pairs)}")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                f"Unexpected error occurred subscribing to order book and trades channels for: {', '.join(self._trading_pairs)}",
                exc_info=True
            )
            raise

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange
        """
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        url = f"{web_utils.wss_url(self._auth.sign_login_action(), self._domain)}"
        await ws.connect(ws_url=url, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        market_id = raw_message["data"]["marketId"]
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=market_id)

        for trade in raw_message["data"]["trades"]:
            trade_message: OrderBookMessage = ChainringOrderBook.trade_message_from_exchange(
                trading_pair=trading_pair, trade=trade
            )
            message_queue.put_nowait(trade_message)
        pass

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        market_id = raw_message["data"]["marketId"]
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=market_id)

        order_book_message: OrderBookMessage = ChainringOrderBook.snapshot_message_from_exchange(
            msg=raw_message["data"], timestamp=time.time(), metadata={"trading_pair": trading_pair}
        )
        message_queue.put_nowait(order_book_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        # diff is not supported by chainring so far
        pass

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = event_message["topic"]["type"]
        return self._snapshot_messages_queue_key if channel == "OrderBook" else self._trade_messages_queue_key
