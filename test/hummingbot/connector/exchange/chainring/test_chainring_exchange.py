import asyncio
import json
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock

from aioresponses import aioresponses
from aioresponses.core import RequestCall

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.chainring import (
    chainring_constants as CONSTANTS,
    chainring_utils,
    chainring_web_utils as web_utils,
)
from hummingbot.connector.exchange.chainring.chainring_exchange import ChainringExchange
from hummingbot.connector.test_support.exchange_connector_test import AbstractExchangeConnectorTests
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCreatedEvent,
)


class ChainringExchangeTests(AbstractExchangeConnectorTests.ExchangeConnectorTests):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.chainring_secret_key = "0xce5715be4e423b41bb3e62bac046f9dc99041c7af3e49492e0f1b44a15de5c4b"  # noqa: mock
        cls.base_asset = "BTC:31338"
        cls.quote_asset = "BTC:31339"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"

    @property
    def all_symbols_url(self):
        return web_utils.public_rest_url(path_url=CONSTANTS.CONFIG_PATH_URL, domain=self.exchange._domain)

    @property
    def latest_prices_url(self):
        return web_utils.public_rest_url(path_url=CONSTANTS.CONFIG_PATH_URL, domain=self.exchange._domain)

    @property
    def network_status_url(self):
        url = web_utils.private_rest_url(CONSTANTS.CONFIG_PATH_URL, domain=self.exchange._domain)
        return url

    @property
    def trading_rules_url(self):
        url = web_utils.private_rest_url(CONSTANTS.CONFIG_PATH_URL, domain=self.exchange._domain)
        return url

    @property
    def order_creation_url(self):
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL, domain=self.exchange._domain)
        return url

    @property
    def balance_url(self):
        url = web_utils.private_rest_url(CONSTANTS.BALANCES_PATH_URL, domain=self.exchange._domain)
        return url

    @property
    def limits_url(self):
        url = web_utils.private_rest_url(CONSTANTS.LIMITS_PATH_URL, domain=self.exchange._domain)
        return url

    @staticmethod
    def server_config_response():
        exchange_info = {
            "chains": [
                {
                    "id": 31338,
                    "name": "Bitlayer",
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
                        },
                        {
                            "name": "DAI:31338",
                            "description": "Dai",
                            "contractAddress": "0x5FC8d32690cc91D4c39d9d3abcBD16989F875707",
                            "decimals": 18,
                            "faucetSupported": True,
                            "iconUrl": "https://chainring-web-icons.s3.us-east-2.amazonaws.com/symbols/dai.svg",
                            "withdrawalFee": "1000000000000000000"
                        },
                        {
                            "name": "ETH:31338",
                            "description": "Ethereum",
                            "contractAddress": "0xCf7Ed3AccA5a467e9e704C703E8D87F634fB0Fc9",
                            "decimals": 18,
                            "faucetSupported": True,
                            "iconUrl": "https://chainring-web-icons.s3.us-east-2.amazonaws.com/symbols/eth.svg",
                            "withdrawalFee": "300000000000000"
                        },
                        {
                            "name": "USDC:31338",
                            "description": "USD Coin",
                            "contractAddress": "0xDc64a140Aa3E981100a9becA4E685f962f0cF6C9",
                            "decimals": 6,
                            "faucetSupported": True,
                            "iconUrl": "https://chainring-web-icons.s3.us-east-2.amazonaws.com/symbols/usdc.svg",
                            "withdrawalFee": "1000000"
                        }
                    ],
                    "jsonRpcUrl": "https://demo-anvil.chainring.co",
                    "blockExplorerNetName": "ChainRing Bitlayer",
                    "blockExplorerUrl": "http://Bitlayer"
                },
                {
                    "id": 31339,
                    "name": "Botanix",
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
                        },
                        {
                            "name": "DAI:31339",
                            "description": "Dai",
                            "contractAddress": "0x5FC8d32690cc91D4c39d9d3abcBD16989F875707",
                            "decimals": 18,
                            "faucetSupported": True,
                            "iconUrl": "https://chainring-web-icons.s3.us-east-2.amazonaws.com/symbols/dai.svg",
                            "withdrawalFee": "1000000000000000000"
                        },
                        {
                            "name": "ETH:31339",
                            "description": "Ethereum",
                            "contractAddress": "0xCf7Ed3AccA5a467e9e704C703E8D87F634fB0Fc9",
                            "decimals": 18,
                            "faucetSupported": True,
                            "iconUrl": "https://chainring-web-icons.s3.us-east-2.amazonaws.com/symbols/eth.svg",
                            "withdrawalFee": "300000000000000"
                        },
                        {
                            "name": "USDC:31339",
                            "description": "USD Coin",
                            "contractAddress": "0xDc64a140Aa3E981100a9becA4E685f962f0cF6C9",
                            "decimals": 6,
                            "faucetSupported": True,
                            "iconUrl": "https://chainring-web-icons.s3.us-east-2.amazonaws.com/symbols/usdc.svg",
                            "withdrawalFee": "1000000"
                        }
                    ],
                    "jsonRpcUrl": "https://demo-anvil2.chainring.co",
                    "blockExplorerNetName": "ChainRing Botanix",
                    "blockExplorerUrl": "http://Botanix"
                }
            ],
            "markets": [
                {
                    "id": "BTC:31338/BTC:31339",
                    "baseSymbol": "BTC:31338",
                    "baseDecimals": 18,
                    "quoteSymbol": "BTC:31339",
                    "quoteDecimals": 18,
                    "tickSize": "0.001000000000000000",
                    "lastPrice": "0.999000000000000000",
                    "minAllowedBidPrice": "0.000000000000000000",
                    "maxAllowedOfferPrice": "2147483647.000000000000000000",
                    "minFee": "5000000000000"
                },
                {
                    "id": "BTC:31338/ETH:31338",
                    "baseSymbol": "BTC:31338",
                    "baseDecimals": 18,
                    "quoteSymbol": "ETH:31338",
                    "quoteDecimals": 18,
                    "tickSize": "0.050000000000000000",
                    "lastPrice": "16.550000000000000000",
                    "minAllowedBidPrice": "0.000000000000000000",
                    "maxAllowedOfferPrice": "2147483647.000000000000000000",
                    "minFee": "10000000000000"
                },
                {
                    "id": "BTC:31338/ETH:31339",
                    "baseSymbol": "BTC:31338",
                    "baseDecimals": 18,
                    "quoteSymbol": "ETH:31339",
                    "quoteDecimals": 18,
                    "tickSize": "0.050000000000000000",
                    "lastPrice": "17.333000000000000000",
                    "minAllowedBidPrice": "0.000000000000000000",
                    "maxAllowedOfferPrice": "2147483647.000000000000000000",
                    "minFee": "10000000000000"
                },
                {
                    "id": "BTC:31338/USDC:31338",
                    "baseSymbol": "BTC:31338",
                    "baseDecimals": 18,
                    "quoteSymbol": "USDC:31338",
                    "quoteDecimals": 6,
                    "tickSize": "25.000000000000000000",
                    "lastPrice": "60812.500000000000000000",
                    "minAllowedBidPrice": "0.000000000000000000",
                    "maxAllowedOfferPrice": "2147483647.000000000000000000",
                    "minFee": "20000"
                },
                {
                    "id": "BTC:31339/ETH:31339",
                    "baseSymbol": "BTC:31339",
                    "baseDecimals": 18,
                    "quoteSymbol": "ETH:31339",
                    "quoteDecimals": 18,
                    "tickSize": "0.050000000000000000",
                    "lastPrice": "17.525000000000000000",
                    "minAllowedBidPrice": "0.000000000000000000",
                    "maxAllowedOfferPrice": "2147483647.000000000000000000",
                    "minFee": "10000000000000"
                },
            ],
            "feeRates": {
                "maker": 100,
                "taker": 200
            }
        }
        return exchange_info

    @property
    def all_symbols_request_mock_response(self):
        return self.server_config_response()

    @property
    def latest_prices_request_mock_response(self):
        return self.server_config_response()

    @property
    def all_symbols_including_invalid_pair_mock_response(self) -> Tuple[str, Any]:
        return "INVALID-PAIR", self.server_config_response()

    @property
    def network_status_request_successful_mock_response(self):
        return {}

    @property
    def trading_rules_request_mock_response(self):
        return self.server_config_response()

    @property
    def trading_rules_request_erroneous_mock_response(self):
        erroneous_mock_response = self.server_config_response().copy()
        del erroneous_mock_response["markets"][0]["tickSize"]
        return erroneous_mock_response

    @property
    def order_creation_request_successful_mock_response(self):
        return {
            "orderId": self.expected_exchange_order_id,
            "requestStatus": "Accepted",
        }

    @property
    def balance_request_mock_response_for_base_and_quote(self):
        return {
            "balances": [
                {
                    "symbol": "BTC:31338",
                    "total": "15000000000000000000",
                    "available": "10000000000000000000",
                },
                {
                    "symbol": "BTC:31339",
                    "total": "2000000000000000000000",
                    "available": "2000000000000000000000",
                }
            ]
        }

    @property
    def limits_request_mock_response_for_base_and_quote(self):
        return {
            "limits": [
                {
                    "marketId": "BTC:31338/BTC:31339",
                    "base": "10000000000000000000",
                    "quote": "2000000000000000000000",
                }
            ]
        }

    @property
    def balance_request_mock_response_only_base(self):
        return {
            "balances": [
                {
                    "symbol": "BTC:31338",
                    "total": "15000000000000000000",
                    "available": "10000000000000000000",
                }
            ]
        }

    @property
    def balance_event_websocket_update(self):
        return {
            "type": "Publish",
            "topic": {
                "type": "Balances"
            },
            "data": {
                "type": "Balances",
                "balances": [
                    {
                        "symbol": "BTC:31338",
                        "total": "15000000000000000000",
                        "available": "10000000000000000000",
                    },
                    {
                        "symbol": "BTC:31339",
                        "total": "2000000000000000000000",
                        "available": "2000000000000000000000",
                    }
                ]
            }
        }

    @property
    def limits_event_websocket_update(self):
        return {
            "type": "Publish",
            "topic": {
                "type": "Limits"
            },
            "data": {
                "type": "Limits",
                "limits": [
                    ["BTC:31338/BTC:31339", "10000000000000000000", "2000000000000000000000"]
                ]
            }
        }

    @aioresponses()
    def test_user_stream_balance_update(self, mock_api):
        # configure symbols response for precision transformation
        self.configure_all_symbols_response(mock_api)

        if self.exchange.real_time_balance_update:
            self.exchange._set_current_timestamp(1640780000)

            balance_event = self.balance_event_websocket_update
            limits_event = self.limits_event_websocket_update

            mock_queue = AsyncMock()
            mock_queue.get.side_effect = [balance_event, limits_event, asyncio.CancelledError]
            self.exchange._user_stream_tracker._user_stream = mock_queue

            try:
                self.async_run_with_timeout(self.exchange._user_stream_event_listener())
            except asyncio.CancelledError:
                pass

            self.assertEqual(Decimal("10"), self.exchange.available_balances[self.base_asset])
            self.assertEqual(Decimal("15"), self.exchange.get_balance(self.base_asset))

    @property
    def expected_latest_price(self):
        return 0.999

    @property
    def expected_supported_order_types(self):
        return [OrderType.LIMIT, OrderType.MARKET]

    @property
    def expected_trading_rule(self):
        min_fee = Decimal(self.trading_rules_request_mock_response["markets"][0]["minFee"])
        maker_fee_rate = Decimal(self.trading_rules_request_mock_response["feeRates"]["maker"]) / 1000000
        base_decimals = self.trading_rules_request_mock_response["markets"][0]["baseDecimals"]
        quote_decimals = self.trading_rules_request_mock_response["markets"][0]["quoteDecimals"]

        return TradingRule(
            trading_pair=self.trading_pair,
            min_price_increment=Decimal(self.trading_rules_request_mock_response["markets"][0]["tickSize"]),
            min_base_amount_increment= (Decimal(1) / Decimal("1e" + str(base_decimals))),
            min_quote_amount_increment=(Decimal(1) / Decimal("1e" + str(quote_decimals))),
            min_notional_size=chainring_utils.move_point_left(min_fee / maker_fee_rate, quote_decimals)
        )

    @property
    def expected_logged_error_for_erroneous_trading_rule(self):
        erroneous_rule = self.trading_rules_request_erroneous_mock_response["markets"][0]
        return f"Error parsing the trading pair rule {erroneous_rule}. Skipping."

    @property
    def expected_exchange_order_id(self):
        return "order_01j3qnn00peayb6w44m3czf200"

    @property
    def is_order_fill_http_update_included_in_status_update(self) -> bool:
        return True

    @property
    def is_order_fill_http_update_executed_during_websocket_order_event_processing(self) -> bool:
        return False

    def _expected_valid_trading_pairs(self):
        return ["BTC:31338-BTC:31339", "BTC:31338-ETH:31338", "BTC:31338-ETH:31339", "BTC:31338-USDC:31338", "BTC:31339-ETH:31339"]

    @property
    def expected_partial_fill_price(self) -> Decimal:
        return Decimal(10500)

    @property
    def expected_partial_fill_amount(self) -> Decimal:
        return Decimal("0.5")

    @property
    def expected_fill_fee(self) -> TradeFeeBase:
        return AddedToCostTradeFee(
            percent_token=self.quote_asset,
            flat_fees=[TokenAmount(token=self.quote_asset, amount=Decimal('0.000349000000000000'))])

    @property
    def expected_fill_trade_id(self) -> str:
        return str(30000)

    def exchange_symbol_for_tokens(self, base_token: str, quote_token: str) -> str:
        return f"{base_token}/{quote_token}"

    def create_exchange_instance(self):
        client_config_map = ClientConfigAdapter(ClientConfigMap())
        return ChainringExchange(
            client_config_map=client_config_map,
            chainring_secret_key="0xce5715be4e423b41bb3e62bac046f9dc99041c7af3e49492e0f1b44a15de5c4b",  # noqa: mock
            trading_pairs=[self.trading_pair],
        )

    @aioresponses()
    def test_update_balances(self, mock_api):
        # configure symbols response for precision transformation
        self.configure_all_symbols_response(mock_api)

        response = self.balance_request_mock_response_for_base_and_quote
        limits_response = self.limits_request_mock_response_for_base_and_quote
        self._configure_balance_response(response=response, mock_api=mock_api)
        self._configure_limits_response(response=limits_response, mock_api=mock_api)

        self.async_run_with_timeout(self.exchange._update_balances())

        available_balances = self.exchange.available_balances
        total_balances = self.exchange.get_all_balances()

        self.assertEqual(Decimal("10"), available_balances[self.base_asset])
        self.assertEqual(Decimal("2000"), available_balances[self.quote_asset])
        self.assertEqual(Decimal("15"), total_balances[self.base_asset])
        self.assertEqual(Decimal("2000"), total_balances[self.quote_asset])

        response = self.balance_request_mock_response_only_base

        self._configure_balance_response(response=response, mock_api=mock_api)
        self.async_run_with_timeout(self.exchange._update_balances())

        available_balances = self.exchange.available_balances
        total_balances = self.exchange.get_all_balances()

        self.assertNotIn(self.quote_asset, available_balances)
        self.assertNotIn(self.quote_asset, total_balances)
        self.assertEqual(Decimal("10"), available_balances[self.base_asset])
        self.assertEqual(Decimal("15"), total_balances[self.base_asset])

    @aioresponses()
    def test_create_buy_limit_order_successfully(self, mock_api):
        # configure symbols response for precision transformation
        self.configure_all_symbols_response(mock_api)

        self._simulate_trading_rules_initialized()
        request_sent_event = asyncio.Event()
        self.exchange._set_current_timestamp(1640780000)

        url = self.order_creation_url

        creation_response = self.order_creation_request_successful_mock_response

        mock_api.post(url,
                      body=json.dumps(creation_response),
                      callback=lambda *args, **kwargs: request_sent_event.set())

        order_id = self.place_buy_order()
        self.async_run_with_timeout(request_sent_event.wait())

        order_request = self._all_executed_requests(mock_api, url)[0]
        self.validate_auth_credentials_present(order_request)
        self.assertIn(order_id, self.exchange.in_flight_orders)
        self.validate_order_creation_request(
            order=self.exchange.in_flight_orders[order_id],
            request_call=order_request)

        create_event: BuyOrderCreatedEvent = self.buy_order_created_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, create_event.timestamp)
        self.assertEqual(self.trading_pair, create_event.trading_pair)
        self.assertEqual(OrderType.LIMIT, create_event.type)
        self.assertEqual(Decimal("100"), create_event.amount)
        self.assertEqual(Decimal("10000"), create_event.price)
        self.assertEqual(order_id, create_event.order_id)
        self.assertEqual(str(self.expected_exchange_order_id), create_event.exchange_order_id)

        self.assertTrue(
            self.is_logged(
                "INFO",
                f"Created {OrderType.LIMIT.name} {TradeType.BUY.name} order {order_id} for "
                f"{Decimal('100.000000')} {self.trading_pair}."
            )
        )

    @aioresponses()
    def test_create_sell_limit_order_successfully(self, mock_api):
        # configure symbols response for precision transformation
        self.configure_all_symbols_response(mock_api)

        self._simulate_trading_rules_initialized()
        request_sent_event = asyncio.Event()
        self.exchange._set_current_timestamp(1640780000)

        url = self.order_creation_url
        creation_response = self.order_creation_request_successful_mock_response

        mock_api.post(url,
                      body=json.dumps(creation_response),
                      callback=lambda *args, **kwargs: request_sent_event.set())

        order_id = self.place_sell_order()
        self.async_run_with_timeout(request_sent_event.wait())

        order_request = self._all_executed_requests(mock_api, url)[0]
        self.validate_auth_credentials_present(order_request)
        self.assertIn(order_id, self.exchange.in_flight_orders)
        self.validate_order_creation_request(
            order=self.exchange.in_flight_orders[order_id],
            request_call=order_request)

        create_event: SellOrderCreatedEvent = self.sell_order_created_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, create_event.timestamp)
        self.assertEqual(self.trading_pair, create_event.trading_pair)
        self.assertEqual(OrderType.LIMIT, create_event.type)
        self.assertEqual(Decimal("100"), create_event.amount)
        self.assertEqual(Decimal("10000"), create_event.price)
        self.assertEqual(order_id, create_event.order_id)
        self.assertEqual(str(self.expected_exchange_order_id), create_event.exchange_order_id)

        self.assertTrue(
            self.is_logged(
                "INFO",
                f"Created {OrderType.LIMIT.name} {TradeType.SELL.name} order {order_id} for "
                f"{Decimal('100.000000')} {self.trading_pair}."
            )
        )

    @aioresponses()
    def test_create_order_fails_and_raises_failure_event(self, mock_api):
        # configure symbols response for precision transformation
        self.configure_all_symbols_response(mock_api)

        self._simulate_trading_rules_initialized()
        request_sent_event = asyncio.Event()
        self.exchange._set_current_timestamp(1640780000)
        url = self.order_creation_url
        mock_api.post(url,
                      status=400,
                      callback=lambda *args, **kwargs: request_sent_event.set())

        order_id = self.place_buy_order()
        self.async_run_with_timeout(request_sent_event.wait())

        order_request = self._all_executed_requests(mock_api, url)[0]
        self.validate_auth_credentials_present(order_request)
        self.assertNotIn(order_id, self.exchange.in_flight_orders)
        order_to_validate_request = InFlightOrder(
            client_order_id=order_id,
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("100"),
            creation_timestamp=self.exchange.current_timestamp,
            price=Decimal("10000")
        )
        self.validate_order_creation_request(
            order=order_to_validate_request,
            request_call=order_request)

        self.assertEquals(0, len(self.buy_order_created_logger.event_log))
        failure_event: MarketOrderFailureEvent = self.order_failure_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, failure_event.timestamp)
        self.assertEqual(OrderType.LIMIT, failure_event.order_type)
        self.assertEqual(order_id, failure_event.order_id)

        self.assertTrue(
            self.is_logged(
                "INFO",
                f"Order {order_id} has failed. Order Update: OrderUpdate(trading_pair='{self.trading_pair}', "
                f"update_timestamp={self.exchange.current_timestamp}, new_state={repr(OrderState.FAILED)}, "
                f"client_order_id='{order_id}', exchange_order_id=None, misc_updates=None)"
            )
        )

    @aioresponses()
    def test_create_order_fails_when_trading_rule_error_and_raises_failure_event(self, mock_api):
        # configure symbols response for precision transformation
        self.configure_all_symbols_response(mock_api)

        self._simulate_trading_rules_initialized()
        request_sent_event = asyncio.Event()
        self.exchange._set_current_timestamp(1640780000)

        url = self.order_creation_url
        mock_api.post(url,
                      status=400,
                      callback=lambda *args, **kwargs: request_sent_event.set())

        order_id_for_invalid_order = self.place_buy_order(
            amount=Decimal("0.0001"), price=Decimal("0.0001")
        )
        # The second order is used only to have the event triggered and avoid using timeouts for tests
        order_id = self.place_buy_order()
        self.async_run_with_timeout(request_sent_event.wait(), timeout=3)

        self.assertNotIn(order_id_for_invalid_order, self.exchange.in_flight_orders)
        self.assertNotIn(order_id, self.exchange.in_flight_orders)

        self.assertEquals(0, len(self.buy_order_created_logger.event_log))
        failure_event: MarketOrderFailureEvent = self.order_failure_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, failure_event.timestamp)
        self.assertEqual(OrderType.LIMIT, failure_event.order_type)
        self.assertEqual(order_id_for_invalid_order, failure_event.order_id)

        self.assertTrue(
            self.is_logged(
                "WARNING",
                "Buy order amount 0.0001 is lower than the minimum order "
                "size 0.01. The order will not be created, increase the "
                "amount to be higher than the minimum order size."
            )
        )
        self.assertTrue(
            self.is_logged(
                "INFO",
                f"Order {order_id} has failed. Order Update: OrderUpdate(trading_pair='{self.trading_pair}', "
                f"update_timestamp={self.exchange.current_timestamp}, new_state={repr(OrderState.FAILED)}, "
                f"client_order_id='{order_id}', exchange_order_id=None, misc_updates=None)"
            )
        )

    @aioresponses()
    def test_cancel_order_successfully(self, mock_api):
        # configure symbols response for precision transformation
        self.configure_all_symbols_response(mock_api)

        request_sent_event = asyncio.Event()
        self.exchange._set_current_timestamp(1640780000)

        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=self.exchange_order_id_prefix + "1",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("100"),
            order_type=OrderType.LIMIT,
        )

        self.assertIn(self.client_order_id_prefix + "1", self.exchange.in_flight_orders)
        order: InFlightOrder = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        url = self.configure_successful_cancelation_response(
            order=order,
            mock_api=mock_api,
            callback=lambda *args, **kwargs: request_sent_event.set())

        self.exchange.cancel(trading_pair=order.trading_pair, client_order_id=order.client_order_id)
        self.async_run_with_timeout(request_sent_event.wait())

        if url != "":
            cancel_request = self._all_executed_requests(mock_api, url)[0]
            self.validate_auth_credentials_present(cancel_request)
            self.validate_order_cancelation_request(
                order=order,
                request_call=cancel_request)

        if self.exchange.is_cancel_request_in_exchange_synchronous:
            self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
            self.assertTrue(order.is_cancelled)
            cancel_event: OrderCancelledEvent = self.order_cancelled_logger.event_log[0]
            self.assertEqual(self.exchange.current_timestamp, cancel_event.timestamp)
            self.assertEqual(order.client_order_id, cancel_event.order_id)

            self.assertTrue(
                self.is_logged(
                    "INFO",
                    f"Successfully canceled order {order.client_order_id}."
                )
            )
        else:
            self.assertIn(order.client_order_id, self.exchange.in_flight_orders)
            self.assertTrue(order.is_pending_cancel_confirmation)

    @aioresponses()
    def test_cancel_order_raises_failure_event_when_request_fails(self, mock_api):
        # configure symbols response for precision transformation
        self.configure_all_symbols_response(mock_api)

        request_sent_event = asyncio.Event()
        self.exchange._set_current_timestamp(1640780000)

        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=self.exchange_order_id_prefix + "1",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("100"),
            order_type=OrderType.LIMIT,
        )

        self.assertIn(self.client_order_id_prefix + "1", self.exchange.in_flight_orders)
        order = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        url = self.configure_erroneous_cancelation_response(
            order=order,
            mock_api=mock_api,
            callback=lambda *args, **kwargs: request_sent_event.set())

        self.exchange.cancel(trading_pair=self.trading_pair, client_order_id=self.client_order_id_prefix + "1")
        self.async_run_with_timeout(request_sent_event.wait())

        if url != "":
            cancel_request = self._all_executed_requests(mock_api, url)[0]
            self.validate_auth_credentials_present(cancel_request)
            self.validate_order_cancelation_request(
                order=order,
                request_call=cancel_request)

        self.assertEquals(0, len(self.order_cancelled_logger.event_log))
        self.assertTrue(
            any(
                log.msg.startswith(f"Failed to cancel order {order.client_order_id}")
                for log in self.log_records
            )
        )

    @aioresponses()
    def test_cancel_order_not_found_in_the_exchange(self, mock_api):
        # configure symbols response for precision transformation
        self.configure_all_symbols_response(mock_api)

        self.exchange._set_current_timestamp(1640780000)
        request_sent_event = asyncio.Event()

        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )

        self.assertIn(self.client_order_id_prefix + "1", self.exchange.in_flight_orders)
        order = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        self.configure_order_not_found_error_cancelation_response(
            order=order, mock_api=mock_api, callback=lambda *args, **kwargs: request_sent_event.set()
        )

        self.exchange.cancel(trading_pair=self.trading_pair, client_order_id=self.client_order_id_prefix + "1")
        self.async_run_with_timeout(request_sent_event.wait())

        self.assertFalse(order.is_done)
        self.assertFalse(order.is_failure)
        self.assertFalse(order.is_cancelled)

        self.assertIn(order.client_order_id, self.exchange._order_tracker.all_updatable_orders)
        self.assertEqual(1, self.exchange._order_tracker._order_not_found_records[order.client_order_id])

    @aioresponses()
    def test_cancel_two_orders_with_cancel_all_and_one_fails(self, mock_api):
        # configure symbols response for precision transformation
        self.configure_all_symbols_response(mock_api)

        self.exchange._set_current_timestamp(1640780000)

        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=self.exchange_order_id_prefix + "1",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("100"),
            order_type=OrderType.LIMIT,
        )

        self.assertIn(self.client_order_id_prefix + "1", self.exchange.in_flight_orders)
        order1 = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        self.exchange.start_tracking_order(
            order_id="12",
            exchange_order_id="5",
            trading_pair=self.trading_pair,
            trade_type=TradeType.SELL,
            price=Decimal("11000"),
            amount=Decimal("90"),
            order_type=OrderType.LIMIT,
        )

        self.assertIn("12", self.exchange.in_flight_orders)
        order2 = self.exchange.in_flight_orders["12"]

        urls = self.configure_one_successful_one_erroneous_cancel_all_response(
            successful_order=order1,
            erroneous_order=order2,
            mock_api=mock_api)

        cancellation_results = self.async_run_with_timeout(self.exchange.cancel_all(10))

        for url in urls:
            cancel_request = self._all_executed_requests(mock_api, url)[0]
            self.validate_auth_credentials_present(cancel_request)

        self.assertEqual(2, len(cancellation_results))
        self.assertEqual(CancellationResult(order1.client_order_id, True), cancellation_results[0])
        self.assertEqual(CancellationResult(order2.client_order_id, False), cancellation_results[1])

        if self.exchange.is_cancel_request_in_exchange_synchronous:
            self.assertEqual(1, len(self.order_cancelled_logger.event_log))
            cancel_event: OrderCancelledEvent = self.order_cancelled_logger.event_log[0]
            self.assertEqual(self.exchange.current_timestamp, cancel_event.timestamp)
            self.assertEqual(order1.client_order_id, cancel_event.order_id)

            self.assertTrue(
                self.is_logged(
                    "INFO",
                    f"Successfully canceled order {order1.client_order_id}."
                )
            )

    @aioresponses()
    def test_lost_order_included_in_order_fills_update_and_not_in_order_status_update(self, mock_api):
        # configure symbols response for precision transformation
        self.configure_all_symbols_response(mock_api)

        self.exchange._set_current_timestamp(1640780000)
        request_sent_event = asyncio.Event()

        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order: InFlightOrder = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        for _ in range(self.exchange._order_tracker._lost_order_count_limit + 1):
            self.async_run_with_timeout(
                self.exchange._order_tracker.process_order_not_found(client_order_id=order.client_order_id))

        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)

        self.configure_completely_filled_order_status_response(
            order=order,
            mock_api=mock_api,
            callback=lambda *args, **kwargs: request_sent_event.set())

        if self.is_order_fill_http_update_included_in_status_update:
            trade_url = self.configure_full_fill_trade_response(
                order=order,
                mock_api=mock_api,
                callback=lambda *args, **kwargs: request_sent_event.set())
        else:
            # If the fill events will not be requested with the order status, we need to manually set the event
            # to allow the ClientOrderTracker to process the last status update
            order.completely_filled_event.set()
            request_sent_event.set()

        self.async_run_with_timeout(self.exchange._update_order_status())
        # Execute one more synchronization to ensure the async task that processes the update is finished
        self.async_run_with_timeout(request_sent_event.wait())

        self.async_run_with_timeout(order.wait_until_completely_filled())
        self.assertTrue(order.is_done)
        self.assertTrue(order.is_failure)

        if self.is_order_fill_http_update_included_in_status_update:
            if trade_url:
                trades_request = self._all_executed_requests(mock_api, trade_url)[0]
                self.validate_auth_credentials_present(trades_request)
                self.validate_trades_request(
                    order=order,
                    request_call=trades_request)

            fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
            self.assertEqual(self.exchange.current_timestamp, fill_event.timestamp)
            self.assertEqual(order.client_order_id, fill_event.order_id)
            self.assertEqual(order.trading_pair, fill_event.trading_pair)
            self.assertEqual(order.trade_type, fill_event.trade_type)
            self.assertEqual(order.order_type, fill_event.order_type)
            self.assertEqual(order.price, fill_event.price)
            self.assertEqual(order.amount, fill_event.amount)
            self.assertEqual(self.expected_fill_fee, fill_event.trade_fee)

        self.assertEqual(0, len(self.buy_order_completed_logger.event_log))
        self.assertIn(order.client_order_id, self.exchange._order_tracker.all_fillable_orders)
        self.assertFalse(
            self.is_logged(
                "INFO",
                f"BUY order {order.client_order_id} completely filled."
            )
        )

        request_sent_event.clear()

        # Configure again the response to the order fills request since it is required by lost orders update logic
        self.configure_full_fill_trade_response(
            order=order,
            mock_api=mock_api,
            callback=lambda *args, **kwargs: request_sent_event.set())

        self.async_run_with_timeout(self.exchange._update_lost_orders_status())
        # Execute one more synchronization to ensure the async task that processes the update is finished
        self.async_run_with_timeout(request_sent_event.wait())

        self.assertTrue(order.is_done)
        self.assertTrue(order.is_failure)

        self.assertEqual(1, len(self.order_filled_logger.event_log))
        self.assertEqual(0, len(self.buy_order_completed_logger.event_log))
        self.assertNotIn(order.client_order_id, self.exchange._order_tracker.all_fillable_orders)
        self.assertFalse(
            self.is_logged(
                "INFO",
                f"BUY order {order.client_order_id} completely filled."
            )
        )

    @aioresponses()
    def test_cancel_lost_order_successfully(self, mock_api):
        # configure symbols response for precision transformation
        self.configure_all_symbols_response(mock_api)

        request_sent_event = asyncio.Event()
        self.exchange._set_current_timestamp(1640780000)

        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=self.exchange_order_id_prefix + "1",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("100"),
            order_type=OrderType.LIMIT,
        )

        self.assertIn(self.client_order_id_prefix + "1", self.exchange.in_flight_orders)
        order: InFlightOrder = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        for _ in range(self.exchange._order_tracker._lost_order_count_limit + 1):
            self.async_run_with_timeout(
                self.exchange._order_tracker.process_order_not_found(client_order_id=order.client_order_id))

        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)

        url = self.configure_successful_cancelation_response(
            order=order,
            mock_api=mock_api,
            callback=lambda *args, **kwargs: request_sent_event.set())

        self.async_run_with_timeout(self.exchange._cancel_lost_orders())
        self.async_run_with_timeout(request_sent_event.wait())

        if url:
            cancel_request = self._all_executed_requests(mock_api, url)[0]
            self.validate_auth_credentials_present(cancel_request)
            self.validate_order_cancelation_request(
                order=order,
                request_call=cancel_request)

        if self.exchange.is_cancel_request_in_exchange_synchronous:
            self.assertNotIn(order.client_order_id, self.exchange._order_tracker.lost_orders)
            self.assertFalse(order.is_cancelled)
            self.assertTrue(order.is_failure)
            self.assertEqual(0, len(self.order_cancelled_logger.event_log))
        else:
            self.assertIn(order.client_order_id, self.exchange._order_tracker.lost_orders)
            self.assertTrue(order.is_failure)

    @aioresponses()
    def test_cancel_lost_order_raises_failure_event_when_request_fails(self, mock_api):
        # configure symbols response for precision transformation
        self.configure_all_symbols_response(mock_api)

        request_sent_event = asyncio.Event()
        self.exchange._set_current_timestamp(1640780000)

        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=self.exchange_order_id_prefix + "1",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("100"),
            order_type=OrderType.LIMIT,
        )

        self.assertIn(self.client_order_id_prefix + "1", self.exchange.in_flight_orders)
        order = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        for _ in range(self.exchange._order_tracker._lost_order_count_limit + 1):
            self.async_run_with_timeout(
                self.exchange._order_tracker.process_order_not_found(client_order_id=order.client_order_id))

        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)

        url = self.configure_erroneous_cancelation_response(
            order=order,
            mock_api=mock_api,
            callback=lambda *args, **kwargs: request_sent_event.set())

        self.async_run_with_timeout(self.exchange._cancel_lost_orders())
        self.async_run_with_timeout(request_sent_event.wait())

        if url:
            cancel_request = self._all_executed_requests(mock_api, url)[0]
            self.validate_auth_credentials_present(cancel_request)
            self.validate_order_cancelation_request(
                order=order,
                request_call=cancel_request)

        self.assertIn(order.client_order_id, self.exchange._order_tracker.lost_orders)
        self.assertEquals(0, len(self.order_cancelled_logger.event_log))
        self.assertTrue(
            any(
                log.msg.startswith(f"Failed to cancel order {order.client_order_id}")
                for log in self.log_records
            )
        )

    @aioresponses()
    def test_lost_order_removed_if_not_found_during_order_status_update(self, mock_api):
        # configure symbols response for precision transformation
        self.configure_all_symbols_response(mock_api)

        self.exchange._set_current_timestamp(1640780000)
        request_sent_event = asyncio.Event()

        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order: InFlightOrder = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        for _ in range(self.exchange._order_tracker._lost_order_count_limit + 1):
            self.async_run_with_timeout(
                self.exchange._order_tracker.process_order_not_found(client_order_id=order.client_order_id)
            )

        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)

        # order fills and order and the same API endpoint
        if self.is_order_fill_http_update_included_in_status_update:
            # This is done for completeness reasons (to have a response available for the trades request)
            self.configure_erroneous_http_fill_trade_response(order=order, mock_api=mock_api)

        self.configure_order_not_found_error_order_status_response(
            order=order, mock_api=mock_api, callback=lambda *args, **kwargs: request_sent_event.set()
        )

        self.async_run_with_timeout(self.exchange._update_lost_orders_status())
        # Execute one more synchronization to ensure the async task that processes the update is finished
        self.async_run_with_timeout(request_sent_event.wait())

        self.assertTrue(order.is_done)
        self.assertTrue(order.is_failure)

        self.assertEqual(0, len(self.buy_order_completed_logger.event_log))
        self.assertNotIn(order.client_order_id, self.exchange._order_tracker.all_fillable_orders)

        self.assertFalse(
            self.is_logged("INFO", f"BUY order {order.client_order_id} completely filled.")
        )

    @aioresponses()
    def test_lost_order_removed_after_cancel_status_user_event_received(self, mock_api):
        # configure symbols response for precision transformation
        self.configure_all_symbols_response(mock_api)

        self.exchange._set_current_timestamp(1640780000)
        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        for _ in range(self.exchange._order_tracker._lost_order_count_limit + 1):
            self.async_run_with_timeout(
                self.exchange._order_tracker.process_order_not_found(client_order_id=order.client_order_id))

        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)

        order_event = self.order_event_for_canceled_order_websocket_update(order=order)

        mock_queue = AsyncMock()
        event_messages = [order_event, asyncio.CancelledError]
        mock_queue.get.side_effect = event_messages
        self.exchange._user_stream_tracker._user_stream = mock_queue

        try:
            self.async_run_with_timeout(self.exchange._user_stream_event_listener())
        except asyncio.CancelledError:
            pass

        self.assertNotIn(order.client_order_id, self.exchange._order_tracker.lost_orders)
        self.assertEqual(0, len(self.order_cancelled_logger.event_log))
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        self.assertFalse(order.is_cancelled)
        self.assertTrue(order.is_failure)

    @aioresponses()
    def test_lost_order_user_stream_full_fill_events_are_processed(self, mock_api):
        # configure symbols response for precision transformation
        self.configure_all_symbols_response(mock_api)

        self.exchange._set_current_timestamp(1640780000)
        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        for _ in range(self.exchange._order_tracker._lost_order_count_limit + 1):
            self.async_run_with_timeout(
                self.exchange._order_tracker.process_order_not_found(client_order_id=order.client_order_id))

        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)

        order_event = self.order_event_for_full_fill_websocket_update(order=order)
        trade_event = self.trade_event_for_full_fill_websocket_update(order=order)

        mock_queue = AsyncMock()
        event_messages = []
        if trade_event:
            event_messages.append(trade_event)
        if order_event:
            event_messages.append(order_event)
        event_messages.append(asyncio.CancelledError)
        mock_queue.get.side_effect = event_messages
        self.exchange._user_stream_tracker._user_stream = mock_queue

        if self.is_order_fill_http_update_executed_during_websocket_order_event_processing:
            self.configure_full_fill_trade_response(
                order=order,
                mock_api=mock_api)

        try:
            self.async_run_with_timeout(self.exchange._user_stream_event_listener())
        except asyncio.CancelledError:
            pass
        # Execute one more synchronization to ensure the async task that processes the update is finished
        self.async_run_with_timeout(order.wait_until_completely_filled())

        fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, fill_event.timestamp)
        self.assertEqual(order.client_order_id, fill_event.order_id)
        self.assertEqual(order.trading_pair, fill_event.trading_pair)
        self.assertEqual(order.trade_type, fill_event.trade_type)
        self.assertEqual(order.order_type, fill_event.order_type)
        self.assertEqual(order.price, fill_event.price)
        self.assertEqual(order.amount, fill_event.amount)
        expected_fee = self.expected_fill_fee
        self.assertEqual(expected_fee, fill_event.trade_fee)

        self.assertEqual(0, len(self.buy_order_completed_logger.event_log))
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        self.assertNotIn(order.client_order_id, self.exchange._order_tracker.lost_orders)
        self.assertTrue(order.is_filled)
        self.assertTrue(order.is_failure)

    @aioresponses()
    def test_update_order_status_when_filled(self, mock_api):
        # configure symbols response for precision transformation
        self.configure_all_symbols_response(mock_api)

        self.exchange._set_current_timestamp(1640780000)
        request_sent_event = asyncio.Event()

        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order: InFlightOrder = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        urls = self.configure_completely_filled_order_status_response(
            order=order,
            mock_api=mock_api,
            callback=lambda *args, **kwargs: request_sent_event.set())

        if self.is_order_fill_http_update_included_in_status_update:
            trade_url = self.configure_full_fill_trade_response(
                order=order,
                mock_api=mock_api)
        else:
            # If the fill events will not be requested with the order status, we need to manually set the event
            # to allow the ClientOrderTracker to process the last status update
            order.completely_filled_event.set()
        self.async_run_with_timeout(self.exchange._update_order_status())
        # Execute one more synchronization to ensure the async task that processes the update is finished
        self.async_run_with_timeout(request_sent_event.wait())

        for url in (urls if isinstance(urls, list) else [urls]):
            order_status_request = self._all_executed_requests(mock_api, url)[0]
            self.validate_auth_credentials_present(order_status_request)
            self.validate_order_status_request(
                order=order,
                request_call=order_status_request)

        self.async_run_with_timeout(order.wait_until_completely_filled())
        self.assertTrue(order.is_done)

        if self.is_order_fill_http_update_included_in_status_update:
            self.assertTrue(order.is_filled)
            if trade_url:
                trades_request = self._all_executed_requests(mock_api, trade_url)[0]
                self.validate_auth_credentials_present(trades_request)
                self.validate_trades_request(
                    order=order,
                    request_call=trades_request)

            fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
            self.assertEqual(self.exchange.current_timestamp, fill_event.timestamp)
            self.assertEqual(order.client_order_id, fill_event.order_id)
            self.assertEqual(order.trading_pair, fill_event.trading_pair)
            self.assertEqual(order.trade_type, fill_event.trade_type)
            self.assertEqual(order.order_type, fill_event.order_type)
            self.assertEqual(order.price, fill_event.price)
            self.assertEqual(order.amount, fill_event.amount)
            self.assertEqual(self.expected_fill_fee, fill_event.trade_fee)

        buy_event: BuyOrderCompletedEvent = self.buy_order_completed_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, buy_event.timestamp)
        self.assertEqual(order.client_order_id, buy_event.order_id)
        self.assertEqual(order.base_asset, buy_event.base_asset)
        self.assertEqual(order.quote_asset, buy_event.quote_asset)
        self.assertEqual(
            order.amount if self.is_order_fill_http_update_included_in_status_update else Decimal(0),
            buy_event.base_asset_amount)
        self.assertEqual(
            order.amount * order.price
            if self.is_order_fill_http_update_included_in_status_update
            else Decimal(0),
            buy_event.quote_asset_amount)
        self.assertEqual(order.order_type, buy_event.order_type)
        self.assertEqual(order.exchange_order_id, buy_event.exchange_order_id)
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        self.assertTrue(
            self.is_logged(
                "INFO",
                f"BUY order {order.client_order_id} completely filled."
            )
        )

    @aioresponses()
    def test_update_order_status_when_order_has_not_changed_and_one_partial_fill(self, mock_api):
        # configure symbols response for precision transformation
        self.configure_all_symbols_response(mock_api)

        self.exchange._set_current_timestamp(1640780000)

        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order: InFlightOrder = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        order_url = self.configure_partially_filled_order_status_response(
            order=order,
            mock_api=mock_api)

        if self.is_order_fill_http_update_included_in_status_update:
            trade_url = self.configure_partial_fill_trade_response(
                order=order,
                mock_api=mock_api)

        self.assertTrue(order.is_open)

        self.async_run_with_timeout(self.exchange._update_order_status())

        if order_url:
            order_status_request = self._all_executed_requests(mock_api, order_url)[0]
            self.validate_auth_credentials_present(order_status_request)
            self.validate_order_status_request(
                order=order,
                request_call=order_status_request)

        self.assertTrue(order.is_open)
        self.assertEqual(OrderState.PARTIALLY_FILLED, order.current_state)

        if self.is_order_fill_http_update_included_in_status_update:
            if trade_url:
                trades_request = self._all_executed_requests(mock_api, trade_url)[0]
                self.validate_auth_credentials_present(trades_request)
                self.validate_trades_request(
                    order=order,
                    request_call=trades_request)

            fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
            self.assertEqual(self.exchange.current_timestamp, fill_event.timestamp)
            self.assertEqual(order.client_order_id, fill_event.order_id)
            self.assertEqual(order.trading_pair, fill_event.trading_pair)
            self.assertEqual(order.trade_type, fill_event.trade_type)
            self.assertEqual(order.order_type, fill_event.order_type)
            self.assertEqual(self.expected_partial_fill_price, fill_event.price)
            self.assertEqual(self.expected_partial_fill_amount, fill_event.amount)
            self.assertEqual(self.expected_fill_fee, fill_event.trade_fee)

    @aioresponses()
    def test_user_stream_update_for_order_full_fill(self, mock_api):
        # configure symbols response for precision transformation
        self.configure_all_symbols_response(mock_api)

        self.exchange._set_current_timestamp(1640780000)
        self.exchange.start_tracking_order(
            order_id=self.client_order_id_prefix + "1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("1"),
        )
        order = self.exchange.in_flight_orders[self.client_order_id_prefix + "1"]

        order_event = self.order_event_for_full_fill_websocket_update(order=order)
        trade_event = self.trade_event_for_full_fill_websocket_update(order=order)

        mock_queue = AsyncMock()
        event_messages = []
        if trade_event:
            event_messages.append(trade_event)
        if order_event:
            event_messages.append(order_event)
        event_messages.append(asyncio.CancelledError)
        mock_queue.get.side_effect = event_messages
        self.exchange._user_stream_tracker._user_stream = mock_queue

        if self.is_order_fill_http_update_executed_during_websocket_order_event_processing:
            self.configure_full_fill_trade_response(
                order=order,
                mock_api=mock_api)

        try:
            self.async_run_with_timeout(self.exchange._user_stream_event_listener())
        except asyncio.CancelledError:
            pass
        # Execute one more synchronization to ensure the async task that processes the update is finished
        self.async_run_with_timeout(order.wait_until_completely_filled())

        fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, fill_event.timestamp)
        self.assertEqual(order.client_order_id, fill_event.order_id)
        self.assertEqual(order.trading_pair, fill_event.trading_pair)
        self.assertEqual(order.trade_type, fill_event.trade_type)
        self.assertEqual(order.order_type, fill_event.order_type)
        self.assertEqual(order.price, fill_event.price)
        self.assertEqual(order.amount, fill_event.amount)
        expected_fee = self.expected_fill_fee
        self.assertEqual(expected_fee, fill_event.trade_fee)

        buy_event: BuyOrderCompletedEvent = self.buy_order_completed_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, buy_event.timestamp)
        self.assertEqual(order.client_order_id, buy_event.order_id)
        self.assertEqual(order.base_asset, buy_event.base_asset)
        self.assertEqual(order.quote_asset, buy_event.quote_asset)
        self.assertEqual(order.amount, buy_event.base_asset_amount)
        self.assertEqual(order.amount * fill_event.price, buy_event.quote_asset_amount)
        self.assertEqual(order.order_type, buy_event.order_type)
        self.assertEqual(order.exchange_order_id, buy_event.exchange_order_id)
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        self.assertTrue(order.is_filled)
        self.assertTrue(order.is_done)

        self.assertTrue(
            self.is_logged(
                "INFO",
                f"BUY order {order.client_order_id} completely filled."
            )
        )

    def validate_auth_credentials_present(self, request_call: RequestCall):
        request_headers = request_call.kwargs["headers"]
        self.assertIn("Authorization", request_headers)

    def validate_order_creation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = json.loads(request_call.kwargs["data"])
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_data["marketId"])
        self.assertEqual(order.trade_type.name.capitalize(), request_data["side"])
        self.assertEqual(CONSTANTS.LIMIT, request_data["type"])
        self.assertEqual("fixed", request_data["amount"]["type"])
        self.assertEqual(Decimal("100000000000000000000"), Decimal(request_data["amount"]["value"]))
        self.assertEqual(Decimal("10000"), Decimal(request_data["price"]))

    def validate_order_cancelation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = json.loads(request_call.kwargs["data"])
        self.assertEqual(self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset), request_data["marketId"])
        self.assertEqual(order.trade_type.name.capitalize(), request_data["side"])
        self.assertEqual(Decimal("100000000000000000000"), Decimal(request_data["amount"]))
        self.assertEqual(order.exchange_order_id, request_data["orderId"])

    def validate_order_status_request(self, order: InFlightOrder, request_call: RequestCall):
        # 'order status' is a regular get request with exchange order id in the path
        pass

    def validate_trades_request(self, order: InFlightOrder, request_call: RequestCall):
        # 'trades' is a regular get request with exchange order id in the path
        pass

    def configure_successful_cancelation_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL) + "/" + order.exchange_order_id
        mock_api.delete(url, status=204, callback=callback)
        return url

    def configure_erroneous_cancelation_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL) + "/" + order.exchange_order_id
        mock_api.delete(url, status=500, body=json.dumps({}), callback=callback)
        return url

    def configure_order_not_found_error_cancelation_response(
            self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL) + "/" + order.exchange_order_id
        response = {"errors": [{"reason": CONSTANTS.ERROR_CODE_REJECTED_BY_SEQUENCER, "message": "rejected"}]}

        mock_api.delete(url, status=422, body=json.dumps(response), callback=callback)
        return url

    def configure_one_successful_one_erroneous_cancel_all_response(
            self,
            successful_order: InFlightOrder,
            erroneous_order: InFlightOrder,
            mock_api: aioresponses) -> List[str]:
        """
        :return: a list of all configured URLs for the cancelations
        """
        all_urls = []
        url = self.configure_successful_cancelation_response(order=successful_order, mock_api=mock_api)
        all_urls.append(url)
        url = self.configure_erroneous_cancelation_response(order=erroneous_order, mock_api=mock_api)
        all_urls.append(url)
        return all_urls

    def configure_completely_filled_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL) + "/external:" + order.client_order_id
        response = self._order_status_request_completely_filled_mock_response(order=order)
        mock_api.get(url, status=200, body=json.dumps(response), callback=callback, repeat=True)
        return url

    def configure_canceled_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL) + "/external:" + order.client_order_id
        response = self._order_status_request_canceled_mock_response(order=order)
        mock_api.get(url, status=200, body=json.dumps(response), callback=callback, repeat=True)
        return url

    def configure_erroneous_http_fill_trade_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL) + "/external:" + order.client_order_id
        mock_api.get(url, status=500, body=json.dumps({}), callback=callback, repeat=False)
        return url

    def configure_open_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL) + "/external:" + order.client_order_id
        response = self._order_status_request_open_mock_response(order=order)
        mock_api.get(url, status=200, body=json.dumps(response), callback=callback, repeat=True)
        return url

    def configure_http_error_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL) + "/external:" + order.client_order_id
        response = self._order_status_request_open_mock_response(order=order)
        mock_api.get(url, status=401, body=json.dumps(response), callback=callback, repeat=True)
        return url

    def configure_partially_filled_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL) + "/external:" + order.client_order_id
        response = self._order_status_request_partially_filled_mock_response(order=order)
        mock_api.get(url, status=200, body=json.dumps(response), callback=callback, repeat=True)
        return url

    def configure_order_not_found_error_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> List[str]:
        url = web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL) + "/external:" + order.client_order_id
        response = {"errors": [{"reason": CONSTANTS.ERROR_CODE_ORDER_NOT_FOUND, "message": "Requested order does not exist"}]}
        mock_api.get(url, status=404, body=json.dumps(response), callback=callback, repeat=True)
        return [url]

    def configure_partial_fill_trade_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        # order fills is the same endpoint and order status
        return self.configure_partially_filled_order_status_response(order, mock_api, callback)

    def configure_full_fill_trade_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        # order fills is the same endpoint and order status
        return self.configure_completely_filled_order_status_response(order, mock_api, callback)

    def _configure_limits_response(
            self,
            response: Dict[str, Any],
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = self.limits_url
        mock_api.get(url, status=200, body=json.dumps(response), callback=callback, repeat=True)
        return url

    def order_event_for_new_order_websocket_update(self, order: InFlightOrder):
        return {
            "type": "Publish",
            "topic": {
                "type": "MyOrders"
            },
            "data": {
                "type": "MyOrderCreated",
                "order": {
                    "type": "limit",
                    "id": self.expected_exchange_order_id,
                    "status": "Open",
                    "marketId": "BTC:1337/BTC:1338",
                    "side": "Sell",
                    "amount": "1000000000000000000",
                    "originalAmount": "1000000000000000000",
                    "price": "10000.000000000000000000",
                    "executions": [],
                    "timing": {
                        "createdAt": "2024-07-28T12:47:42.174620Z",
                        "updatedAt": None,
                        "closedAt": None,
                        "sequencerTimeNs": "2150833"
                    }
                }
            }
        }

    def order_event_for_canceled_order_websocket_update(self, order: InFlightOrder):
        return {
            "type": "Publish",
            "topic": {
                "type": "MyOrders"
            },
            "data": {
                "type": "MyOrderUpdated",
                "order": {
                    "type": "limit",
                    "id": self.expected_exchange_order_id,
                    "status": "Cancelled",
                    "marketId": "BTC:1337/BTC:1338",
                    "side": "Buy",
                    "amount": "1000000000000000000",
                    "originalAmount": "1000000000000000000",
                    "price": "1.000000000000000000",
                    "executions": [],
                    "timing": {
                        "createdAt": "2024-07-28T12:44:28.376253Z",
                        "updatedAt": "2024-07-28T12:44:41.645880Z",
                        "closedAt": None,
                        "sequencerTimeNs": "4190625"
                    }
                }
            }
        }

    def order_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
            "type": "Publish",
            "topic": {
                "type": "MyOrders"
            },
            "data": {
                "type": "MyOrderUpdated",
                "order": {
                    "type": "limit",
                    "id": self.expected_exchange_order_id,
                    "status": "Filled",
                    "marketId": "BTC:1337/BTC:1338",
                    "side": "Buy",
                    "amount": "1000000000000000000",
                    "originalAmount": "1000000000000000000",
                    "price": "10000.000000000000000000",
                    "executions": [
                        {
                            "tradeId": "trade_01j3wp1krafngrg3c6a5p0renp",
                            "timestamp": "2024-07-28T12:46:29.628Z",
                            "amount": "1000000000000000000",
                            "price": "10000",
                            "role": "Taker",
                            "feeAmount": "20000000000000000",
                            "feeSymbol": "BTC:1338",
                            "marketId": "BTC:1337/BTC:1338"
                        }
                    ],
                    "timing": {
                        "createdAt": "2024-07-28T12:46:29.631155Z",
                        "updatedAt": "2024-07-28T12:46:29.663397Z",
                        "closedAt": None,
                        "sequencerTimeNs": "2942500"
                    }
                }
            }
        }

    def trade_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
            "type": "Publish",
            "topic": {
                "type": "MyTrades"
            },
            "data": {
                "type": "MyTradesCreated",
                "trades": [
                    {
                        "id": "trade_01j3wp1krafngrg3c6a5p0renp",
                        "timestamp": "2024-07-28T12:46:29.628Z",
                        "orderId": self.expected_exchange_order_id,
                        "marketId": "BTC:31338/BTC:31339",
                        "executionRole": "Maker",
                        "counterOrderId": "order_01j3wp1kqke32b1nqsg8mx1fa1",
                        "side": "Buy",
                        "amount": "1000000000000000000",
                        "price": "10000.000000000000000000",
                        "feeAmount": "349000000000000",
                        "feeSymbol": "BTC:31339",
                        "settlementStatus": "Pending"
                    }
                ]
            }
        }

    @aioresponses()
    def test_get_last_trade_prices(self, mock_api):
        self.configure_all_symbols_response(mock_api)
        self.configure_trading_rules_response(mock_api)
        mock_api.get(self.latest_prices_url, body=json.dumps(self.latest_prices_request_mock_response))

        latest_prices: Dict[str, float] = self.async_run_with_timeout(
            self.exchange.get_last_traded_prices(trading_pairs=[self.trading_pair])
        )

        self.assertEqual(1, len(latest_prices))
        self.assertEqual(self.expected_latest_price, latest_prices[self.trading_pair])

    def test_format_trading_rules(self):
        exchange_info = self.server_config_response()
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(exchange_info)
        result = self.async_run_with_timeout(self.exchange._format_trading_rules(exchange_info))

        btc_btc = result[0]
        self.assertEqual(btc_btc.trading_pair, "BTC:31338-BTC:31339")
        self.assertEqual(btc_btc.min_order_size, Decimal("0"))
        self.assertEqual(btc_btc.max_order_size, Decimal("1E+56"))
        self.assertEqual(btc_btc.min_price_increment, Decimal("0.001"))
        self.assertEqual(btc_btc.min_base_amount_increment, Decimal("1E-18"))
        self.assertEqual(btc_btc.min_quote_amount_increment, Decimal("1E-18"))
        self.assertEqual(btc_btc.min_notional_size, Decimal("0.05"))
        self.assertEqual(btc_btc.min_order_value, Decimal("0"))
        self.assertEqual(btc_btc.max_price_significant_digits, Decimal("1E+56"))
        self.assertEqual(btc_btc.supports_limit_orders, True)
        self.assertEqual(btc_btc.supports_market_orders, True)
        self.assertEqual(btc_btc.supports_market_orders, True)
        self.assertEqual(btc_btc.buy_order_collateral_token, "BTC:31339")
        self.assertEqual(btc_btc.sell_order_collateral_token, "BTC:31339")

    def _order_status_request_completely_filled_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "type": CONSTANTS.LIMIT,
            "id": order.exchange_order_id,
            "status": "Filled",
            "marketId": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "side": CONSTANTS.SIDE_BUY,
            "amount": "1000000000000000000",
            "originalAmount": "1000000000000000000",
            "executions": [
                {
                    "tradeId": "trade_01j3sfzk6yftmr0qqc8jz1cwrn",
                    "timestamp": "2024-07-27T07:02:48.793Z",
                    "amount": "1000000000000000000",
                    "price": "10000",
                    "role": "Taker",
                    "feeAmount": "349000000000000",
                    "feeSymbol": self.quote_asset,
                    "marketId": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                }
            ],
            "timing": {
                "createdAt": "2024-07-27T07:02:48.795212Z",
                "updatedAt": "2024-07-27T07:02:48.803961Z",
                "closedAt": None,
                "sequencerTimeNs": "162773"
            }
        }

    def _order_status_request_canceled_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "type": CONSTANTS.LIMIT,
            "id": order.exchange_order_id,
            "status": "Cancelled",
            "marketId": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "side": CONSTANTS.SIDE_BUY,
            "amount": "1000000000000000000",
            "originalAmount": "1000000000000000000",
            "executions": [],
            "timing": {
                "createdAt": "2024-07-27T07:02:48.795212Z",
                "updatedAt": None,
                "closedAt": None,
                "sequencerTimeNs": "162773"
            }
        }

    def _order_status_request_open_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "type": CONSTANTS.LIMIT,
            "id": order.exchange_order_id,
            "status": "Open",
            "marketId": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "side": CONSTANTS.SIDE_BUY,
            "amount": "1000000000000000000",
            "originalAmount": "1000000000000000000",
            "executions": [],
            "timing": {
                "createdAt": "2024-07-27T07:02:48.795212Z",
                "updatedAt": None,
                "closedAt": None,
                "sequencerTimeNs": "162773"
            }
        }

    def _order_status_request_partially_filled_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "type": CONSTANTS.LIMIT,
            "id": order.exchange_order_id,
            "status": "Partial",
            "marketId": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            "side": CONSTANTS.SIDE_BUY,
            "amount": "1000000000000000000",
            "originalAmount": "1000000000000000000",
            "executions": [
                {
                    "tradeId": "trade_01j3sfzk6yftmr0qqc8jz1cwrn",
                    "timestamp": "2024-07-27T07:02:48.793Z",
                    "amount": "500000000000000000",
                    "price": "10500",
                    "role": "Taker",
                    "feeAmount": "349000000000000",
                    "feeSymbol": self.quote_asset,
                    "marketId": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                }
            ],
            "timing": {
                "createdAt": "2024-07-27T07:02:48.795212Z",
                "updatedAt": "2024-07-27T07:02:48.803961Z",
                "closedAt": None,
                "sequencerTimeNs": "162773"
            }
        }
