import asyncio
import json
import unittest
from typing import Awaitable, Optional
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.chainring import chainring_constants as CONSTANTS, chainring_web_utils as web_utils
from hummingbot.connector.exchange.chainring.chainring_api_user_stream_data_source import (
    ChainringAPIUserStreamDataSource,
)
from hummingbot.connector.exchange.chainring.chainring_auth import ChainringAuth
from hummingbot.connector.exchange.chainring.chainring_exchange import ChainringExchange
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler


class TestChainringAPIUserStreamDataSource(unittest.TestCase):
    # the level is required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.base_asset = "BTC:31338"
        cls.quote_asset = "BTC:31339"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.domain = CONSTANTS.DEFAULT_DOMAIN

    def setUp(self) -> None:
        super().setUp()

        self.log_records = []
        self.listening_task: Optional[asyncio.Task] = None
        self.mocking_assistant = NetworkMockingAssistant()

        self.throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS)
        self.mock_time_provider = MagicMock()
        self.mock_time_provider.time.return_value = 1000

        # NOTE: RANDOM KEYS GENERATED JUST FOR UNIT TESTS
        self.auth = ChainringAuth(
            secret_key="0xce5715be4e423b41bb3e62bac046f9dc99041c7af3e49492e0f1b44a15de5c4b",  # noqa: mock
            domain=CONSTANTS.DEFAULT_DOMAIN,
        )

        client_config_map = ClientConfigAdapter(ClientConfigMap())
        self.connector = ChainringExchange(
            client_config_map,
            chainring_secret_key="0xce5715be4e423b41bb3e62bac046f9dc99041c7af3e49492e0f1b44a15de5c4b",  # noqa: mock
            trading_pairs=[self.trading_pair],
            domain=self.domain,
        )

        self.api_factory = web_utils.build_api_factory(throttler=self.throttler, auth=self.auth)

        self.data_source = ChainringAPIUserStreamDataSource(
            auth=self.auth,
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.api_factory,
            domain=self.domain,
        )

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message for record in self.log_records)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_subscribe_to_user_stream_events(self, ws_connect_mock):
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()

        expected_messages = [
            {"type": "Subscribe", "topic": {"type": "Balances"}},
            {"type": "Subscribe", "topic": {"type": "Limits"}},
            {"type": "Subscribe", "topic": {"type": "MyOrders"}},
            {"type": "Subscribe", "topic": {"type": "MyTrades"}},
        ]

        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(""))

        output_queue = asyncio.Queue()
        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_user_stream(output=output_queue))

        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)

        # verify subscription messages were sent
        sent_subscription_messages = self.mocking_assistant.json_messages_sent_through_websocket(
            websocket_mock=ws_connect_mock.return_value)
        self.assertEqual(len(expected_messages), len(sent_subscription_messages))
        for expected, sent in zip(expected_messages, sent_subscription_messages):
            self.assertEqual(expected, sent)

        # verify the appropriate log was made
        self.assertTrue(self._is_logged("INFO", "Subscribed to user streams..."))

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_does_not_queue_unknown_event(self, mock_ws):
        unknown_event = {"type": "unknown_event"}
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        self.mocking_assistant.add_websocket_aiohttp_message(mock_ws.return_value, json.dumps(unknown_event))

        msg_queue = asyncio.Queue()
        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_user_stream(msg_queue))

        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(mock_ws.return_value)

        self.assertEqual(0, msg_queue.qsize())

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream(self, mock_ws):
        balamncesEvent = {"type": "Publish", "topic": {"type": "Balances"}, "data": {"type": "Balances", "balances": []}}

        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        self.mocking_assistant.add_websocket_aiohttp_message(mock_ws.return_value, json.dumps(balamncesEvent))

        msg_queue = asyncio.Queue()
        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_user_stream(msg_queue))

        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(mock_ws.return_value)

        self.assertEqual(1, msg_queue.qsize())

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    @patch("hummingbot.core.data_type.user_stream_tracker_data_source.UserStreamTrackerDataSource._sleep")
    def test_listen_for_user_stream_iter_message_throws_exception(self, sleep_mock, mock_ws):
        msg_queue: asyncio.Queue = asyncio.Queue()
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        mock_ws.return_value.receive.side_effect = Exception("TEST ERROR")
        sleep_mock.side_effect = asyncio.CancelledError  # to finish the task execution

        try:
            self.async_run_with_timeout(self.data_source.listen_for_user_stream(msg_queue))
        except asyncio.CancelledError:
            pass

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error while listening to user stream. Retrying after 5 seconds...")
        )
