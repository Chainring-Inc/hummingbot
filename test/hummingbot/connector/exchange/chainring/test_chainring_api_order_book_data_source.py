import asyncio
import json
import unittest
import urllib.parse
from decimal import Decimal
from typing import Awaitable, Dict, Optional
from unittest.mock import AsyncMock, patch

from aioresponses import aioresponses

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.chainring import chainring_constants as CONSTANTS
from hummingbot.connector.exchange.chainring.chainring_api_order_book_data_source import ChainringAPIOrderBookDataSource
from hummingbot.connector.exchange.chainring.chainring_exchange import ChainringExchange
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class TestChainringAPIOrderBookDataSource(unittest.TestCase):
    # logging.Level required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.base_asset = "BTC:31338"
        cls.quote_asset = "BTC:31339"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.base_asset}/{cls.quote_asset}"
        cls.domain = CONSTANTS.DEFAULT_DOMAIN

    def setUp(self) -> None:
        super().setUp()

        self.log_records = []
        self.async_task: Optional[asyncio.Task] = None
        self.mocking_assistant = NetworkMockingAssistant()
        client_config_map = ClientConfigAdapter(ClientConfigMap())

        # NOTE: RANDOM KEYS GENERATED JUST FOR UNIT TESTS
        self.connector = ChainringExchange(
            client_config_map,
            chainring_secret_key="0xce5715be4e423b41bb3e62bac046f9dc99041c7af3e49492e0f1b44a15de5c4b",  # noqa: mock
            trading_pairs=[self.trading_pair],
            domain=self.domain,
        )

        self.throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS)
        self.time_synchronizer = TimeSynchronizer()
        self.time_synchronizer.add_time_offset_ms_sample(1000)
        self.ob_data_source = ChainringAPIOrderBookDataSource(
            auth=self.connector._auth,
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
            domain=self.domain,
            time_synchronizer=self.time_synchronizer
        )

        self.connector._initialize_trading_pair_symbols_from_exchange_info(self.valid_exchange_info())

        self._original_full_order_book_reset_time = self.ob_data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS
        self.ob_data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = -1

        self.ob_data_source.logger().setLevel(1)
        self.ob_data_source.logger().addHandler(self)
        self.resume_test_event = asyncio.Event()

    def tearDown(self) -> None:
        self.async_task and self.async_task.cancel()
        self.ob_data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = self._original_full_order_book_reset_time
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message for record in self.log_records)

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def valid_exchange_info(self) -> Dict:
        exchange_rules = {
            "chains": [
                {
                    "id": 31338,
                    "name": "31338",
                    "contracts": [
                        {
                            "name": "Exchange",
                            "address": "0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512"
                        }
                    ],
                    "symbols": [
                        {
                            "name": "BTC:31338",
                            "description": "Bitcoin",
                            "contractAddress": None,
                            "decimals": 18,
                            "faucetSupported": True,
                            "iconUrl": "https://chainring-web-icons.s3.us-east-2.amazonaws.com/symbols/btc.svg",
                            "withdrawalFee": "20000000000000"
                        }
                    ]
                },
                {
                    "id": 31339,
                    "name": "31339",
                    "contracts": [
                        {
                            "name": "Exchange",
                            "address": "0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512"
                        }
                    ],
                    "symbols": [
                        {
                            "name": "BTC:31339",
                            "description": "Bitcoin",
                            "contractAddress": None,
                            "decimals": 18,
                            "faucetSupported": True,
                            "iconUrl": "https://chainring-web-icons.s3.us-east-2.amazonaws.com/symbols/btc.svg",
                            "withdrawalFee": "20000000000000"
                        }
                    ]
                }
            ],
            "markets": [
                {
                    "id": "BTC:31338/BTC:31339",
                    "baseSymbol": "BTC:31338",
                    "baseDecimals": 18,
                    "quoteSymbol": "BTC:31339",
                    "quoteDecimals": 18,
                    "tickSize": "0.000100000000000000",
                    "lastPrice": "1.000000000000000000",
                    "minAllowedBidPrice": "0.000100000000000000",
                    "maxAllowedOfferPrice": "2147483647.000000000000000000",
                    "minFee": "10000000000000"
                }
            ],
            "feeRates": {
                "maker": 100,
                "taker": 200
            }
        }
        return exchange_rules

    @staticmethod
    def _api_snapshot_response() -> Dict[str, any]:
        snapshot = {
            "marketId": "BTC:31338/BTC:31339",
            "buy": [
                {
                    "price": "18.350",
                    "size": "0.056635459831565037"
                },
            ],
            "sell": [
                {
                    "price": "18.450",
                    "size": "0.242130551905629504"
                }
            ],
            "last": {
                "price": "18.400",
                "direction": "Down"
            }
        }
        return snapshot

    def _ws_snapshot_event(self) -> Dict[str, any]:
        api_snapshot = self._api_snapshot_response()
        ws_snapshot = {
            "type": "Publish",
            "topic": {
                "type": "OrderBook",
                "marketId": "BTC:31338/BTC:31339"
            },
            "data": {
                "type": "OrderBook",
                "marketId": api_snapshot.get("marketId"),
                "buy": api_snapshot.get("buy"),
                "sell": api_snapshot.get("sell"),
                "last": api_snapshot.get("last"),
            }
        }
        return ws_snapshot

    @staticmethod
    def _market_trades_event() -> Dict[str, any]:
        return {
            "type": "Publish",
            "topic": {
                "type": "MarketTrades",
                "marketId": "BTC:31338/BTC:31339"
            },
            "data": {
                "type": "MarketTradesCreated",
                "marketId": "BTC:31338/BTC:31339",
                "trades": [
                    [
                        "trade_01j3zaf8ncfk4bdftq39qbc9rj",
                        "Buy",
                        "9935047605818776",
                        "1.005000000000000000",
                        1722259317416
                    ]
                ]
            }
        }

    @aioresponses()
    def test_get_order_book_snapshot(self, mock_api):
        escaped_market_id = urllib.parse.quote_plus(self.ex_trading_pair, safe='')
        order_book_url_path = CONSTANTS.ORDER_BOOK_PATH_URL.format(escaped_market_id)
        url = f"{CONSTANTS.REST_URLS[self.domain]}{order_book_url_path}"
        mock_api.get(url, body=json.dumps(self._api_snapshot_response()))

        ret = self.async_run_with_timeout(coroutine=self.ob_data_source._order_book_snapshot(self.trading_pair))

        self.assertEqual("BTC:31338-BTC:31339", ret.trading_pair)
        self.assertEqual(OrderBookMessageType.SNAPSHOT, ret.type)
        self.assertEqual(-1, ret.trade_id)

        self.assertEqual(1, len(ret.bids))
        self.assertEqual(18.35, ret.bids[0].price)
        self.assertEqual(0.056635459831565037, ret.bids[0].amount)

        self.assertEqual(1, len(ret.asks))
        self.assertEqual(18.450, ret.asks[0].price)
        self.assertEqual(0.242130551905629504, ret.asks[0].amount)

    @aioresponses()
    def test_get_new_order_book(self, mock_api):
        escaped_market_id = urllib.parse.quote_plus(self.ex_trading_pair, safe='')
        order_book_url_path = CONSTANTS.ORDER_BOOK_PATH_URL.format(escaped_market_id)
        url = f"{CONSTANTS.REST_URLS[self.domain]}{order_book_url_path}"
        mock_api.get(url, body=json.dumps(self._api_snapshot_response()))

        ret = self.async_run_with_timeout(coroutine=self.ob_data_source.get_new_order_book(self.trading_pair))

        self.assertTrue(isinstance(ret, OrderBook))

    @aioresponses()
    def test_listen_for_order_book_snapshots(self, mock_api):
        escaped_market_id = urllib.parse.quote_plus(self.ex_trading_pair, safe='')
        order_book_url_path = CONSTANTS.ORDER_BOOK_PATH_URL.format(escaped_market_id)
        url = f"{CONSTANTS.REST_URLS[self.domain]}{order_book_url_path}"
        mock_api.get(url, body=json.dumps(self._api_snapshot_response()))

        output_queue = asyncio.Queue()
        self.async_task = self.ev_loop.create_task(self.ob_data_source.listen_for_order_book_snapshots(self.ev_loop, output_queue))

        msg = self.async_run_with_timeout(coroutine=output_queue.get())

        self.assertTrue(isinstance(msg, OrderBookMessage))

    @aioresponses()
    def test_get_snapshot_raises(self, mock_api):
        escaped_market_id = urllib.parse.quote_plus(self.ex_trading_pair, safe='')
        order_book_url_path = CONSTANTS.ORDER_BOOK_PATH_URL.format(escaped_market_id)
        url = f"{CONSTANTS.REST_URLS[self.domain]}{order_book_url_path}"
        mock_api.get(url, status=500)

        with self.assertRaises(IOError):
            self.async_run_with_timeout(coroutine=self.ob_data_source._order_book_snapshot(self.trading_pair))

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_subscriptions_subscribes_to_order_book(self, ws_connect_mock):
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()

        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(""))

        self.listening_task = self.ev_loop.create_task(self.ob_data_source.listen_for_subscriptions())

        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)

        sent_subscription_messages = self.mocking_assistant.json_messages_sent_through_websocket(
            websocket_mock=ws_connect_mock.return_value)

        self.assertEqual(2, len(sent_subscription_messages))

        expected_order_book_subscription = {
            "type": "Subscribe",
            "topic": {
                "type": "OrderBook",
                "marketId": self.ex_trading_pair
            }
        }
        self.assertEqual(expected_order_book_subscription, sent_subscription_messages[0])

        expected_market_trades_subscription = {
            "type": "Subscribe",
            "topic": {
                "type": "MarketTrades",
                "marketId": self.ex_trading_pair
            }
        }
        self.assertEqual(expected_market_trades_subscription, sent_subscription_messages[1])

        self.assertTrue(self._is_logged(
            "INFO",
            f"Subscribed to order book and trade channels for: {self.trading_pair}"
        ))

    @patch("hummingbot.core.data_type.order_book_tracker_data_source.OrderBookTrackerDataSource._sleep")
    @patch("aiohttp.ClientSession.ws_connect")
    def test_listen_for_subscriptions_raises_cancel_exception(self, mock_ws, _: AsyncMock):
        mock_ws.side_effect = asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = self.ev_loop.create_task(self.ob_data_source.listen_for_subscriptions())
            self.async_run_with_timeout(self.listening_task)

    @patch("hummingbot.core.data_type.order_book_tracker_data_source.OrderBookTrackerDataSource._sleep")
    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_subscriptions_logs_exception_details(self, mock_ws, sleep_mock):
        mock_ws.side_effect = Exception("TEST ERROR.")
        sleep_mock.side_effect = lambda _: self._create_exception_and_unlock_test_with_event(asyncio.CancelledError())

        self.listening_task = self.ev_loop.create_task(self.ob_data_source.listen_for_subscriptions())

        self.async_run_with_timeout(self.resume_test_event.wait())

        self.assertTrue(
            self._is_logged(
                "ERROR",
                "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds..."))

    def test_parse_order_book_snapshot_message(self):
        output_queue: asyncio.Queue = asyncio.Queue()
        self.async_task = self.ev_loop.create_task(
            self.ob_data_source._parse_order_book_snapshot_message(self._ws_snapshot_event(), output_queue)
        )
        self.async_run_with_timeout(self.async_task)
        self.ob_data_source._sleep(1)

        msg: OrderBookMessage = self.async_run_with_timeout(output_queue.get())
        self.assertTrue(isinstance(msg, OrderBookMessage))

    def test_parse_order_book_diff_message(self):
        output_queue: asyncio.Queue = asyncio.Queue()
        self.async_task = self.ev_loop.create_task(
            self.ob_data_source._parse_order_book_diff_message({}, output_queue)
        )
        self.async_run_with_timeout(self.async_task)

        # order book diffs are not supported
        self.assertTrue(output_queue.empty())

    def test_parse_trade_message(self):
        output_queue: asyncio.Queue = asyncio.Queue()
        self.async_task = self.ev_loop.create_task(
            self.ob_data_source._parse_trade_message(self._market_trades_event(), output_queue)
        )
        self.async_run_with_timeout(self.async_task)

        # market trades are not supported
        self.assertTrue(1, output_queue.qsize())

        msg = output_queue.get_nowait()
        self.assertTrue(isinstance(msg, OrderBookMessage))

        market_trade = msg.content
        self.assertTrue("BTC:31338-BTC:31339", market_trade["trading_pair"])
        self.assertTrue("trade_01j3zaf8ncfk4bdftq39qbc9rj", market_trade["trade_id"])
        self.assertTrue(TradeType.BUY, market_trade["trade_type"])
        self.assertTrue(Decimal("9935047605818776"), market_trade["amount"])
        self.assertTrue(Decimal("1.005000000000000000"), market_trade["price"])

    @aioresponses()
    def test_listen_for_order_book_snapshots_successful(self, mock_api):
        escaped_market_id = urllib.parse.quote_plus(self.ex_trading_pair, safe='')
        order_book_url_path = CONSTANTS.ORDER_BOOK_PATH_URL.format(escaped_market_id)
        url = f"{CONSTANTS.REST_URLS[self.domain]}{order_book_url_path}"
        mock_api.get(url, body=json.dumps(self._api_snapshot_response()))

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.ev_loop.create_task(
            self.ob_data_source.listen_for_order_book_snapshots(self.ev_loop, msg_queue)
        )

        msg: OrderBookMessage = self.async_run_with_timeout(msg_queue.get())
        self.assertTrue(isinstance(msg, OrderBookMessage))
