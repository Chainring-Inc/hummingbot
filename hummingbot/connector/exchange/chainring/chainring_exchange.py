import asyncio
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from aiohttp import ContentTypeError
from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.chainring import (
    chainring_constants as CONSTANTS,
    chainring_utils,
    chainring_web_utils,
)
from hummingbot.connector.exchange.chainring.chainring_api_order_book_data_source import ChainringAPIOrderBookDataSource
from hummingbot.connector.exchange.chainring.chainring_api_user_stream_data_source import (
    ChainringAPIUserStreamDataSource,
)
from hummingbot.connector.exchange.chainring.chainring_auth import ChainringAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import (
    AddedToCostTradeFee,
    DeductedFromReturnsTradeFee,
    TokenAmount,
    TradeFeeBase,
)
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class ChainringExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    web_utils = chainring_web_utils

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 chainring_secret_key: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        self._secret_key = chainring_secret_key
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._last_trades_poll_chainring_timestamp = 1.0
        self._exchange_info: Optional[Dict[str, Any]] = None
        super().__init__(client_config_map)

    @property
    def authenticator(self):
        return ChainringAuth(
            secret_key=self._secret_key,
            domain=self._domain,
        )

    @property
    def name(self) -> str:
        return "chainring"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return CONSTANTS.DEFAULT_DOMAIN

    @property
    def client_order_id_max_length(self):
        return 32

    #
    @property
    def client_order_id_prefix(self):
        return ""

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.CONFIG_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.CONFIG_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.CONFIG_PATH_URL

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    def supported_order_types(self) -> List[OrderType]:
        """
        :return a list of OrderType supported by this connector
        """
        return [OrderType.LIMIT, OrderType.MARKET]

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return False  # time synchronizer is not required for ChainRing

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return CONSTANTS.ERROR_CODE_ORDER_NOT_FOUND in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return CONSTANTS.ERROR_CODE_REJECTED_BY_SEQUENCER in str(cancelation_exception)

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Example:
        {
            "markets": [
                {
                    "id": "BTC:1337/BTC:1338",
                    "baseSymbol": "BTC:1337",
                    "baseDecimals": 18,
                    "quoteSymbol": "BTC:1338",
                    "quoteDecimals": 18,
                    "tickSize": "0.001000000000000000",
                    "lastPrice": "1.000500000000000000",
                    "minAllowedBidPrice": "0.000000000000000000",
                    "maxAllowedOfferPrice": "2147483647.000000000000000000",
                    "minFee": "5000000000000"
                }
            ]
        ]
        """
        retval = []
        if chainring_utils.is_exchange_information_valid(exchange_info_dict):
            markets = exchange_info_dict.get("markets", [])
            for market in markets:
                try:
                    retval.append(
                        TradingRule(
                            trading_pair=await self.trading_pair_associated_to_exchange_symbol(symbol=market.get("id")),
                            min_price_increment=Decimal(market["tickSize"]),
                            min_base_amount_increment=(Decimal(1) / Decimal("1e" + str(market["baseDecimals"]))),
                            min_quote_amount_increment=(Decimal(1) / Decimal("1e" + str(market["quoteDecimals"])))
                        )
                    )
                except Exception:
                    self.logger().error(f"Error parsing the trading pair rule {market}. Skipping.", exc_info=True)
        return retval

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return chainring_web_utils.build_api_factory(throttler=self._throttler, auth=self._auth)

    def trading_pair_symbol_map_ready(self):
        """
        also include check of _symbols_precision
        """
        return super().trading_pair_symbol_map_ready() and self._exchange_info is not None and len(self._exchange_info) > 0

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        if chainring_utils.is_exchange_information_valid(exchange_info):
            mapping = bidict()
            for market_data in exchange_info["markets"]:
                mapping[market_data["id"]] = combine_to_hb_trading_pair(base=market_data["baseSymbol"],
                                                                        quote=market_data["quoteSymbol"])
            self._set_trading_pair_symbol_map(mapping)
            self._exchange_info = exchange_info

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return ChainringAPIOrderBookDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory,
            time_synchronizer=self._time_synchronizer
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return ChainringAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None,
                 ) -> TradeFeeBase:
        is_maker = is_maker or False

        if CONSTANTS.FEES_KEY not in self._trading_fees.keys():
            fee = build_trade_fee(
                self.name,
                is_maker,
                base_currency=base_currency,
                quote_currency=quote_currency,
                order_type=order_type,
                order_side=order_side,
                amount=amount,
                price=price,
            )
        else:
            fee_data = self._trading_fees[CONSTANTS.FEES_KEY]
            if is_maker:
                fee_value = Decimal(fee_data[CONSTANTS.FEE_MAKER_KEY])
            else:
                fee_value = Decimal(fee_data[CONSTANTS.FEE_TAKER_KEY])

            if order_side == TradeType.BUY:
                fee = AddedToCostTradeFee(percent=fee_value, percent_token=quote_currency)
            else:
                fee = DeductedFromReturnsTradeFee(percent=fee_value, percent_token=quote_currency)

        return fee

    async def _update_trading_fees(self):
        response: Dict[str, Dict[str, Any]] = await self._api_get(
            path_url=CONSTANTS.CONFIG_PATH_URL,
            is_auth_required=False,
            limit_id=CONSTANTS.ALL_HTTP
        )

        # decimal representation of percents
        maker_fee_percents = Decimal(response["feeRates"]["maker"]) / 1_000_000
        taker_fee_percents = Decimal(response["feeRates"]["taker"]) / 1_000_000

        self._trading_fees[CONSTANTS.FEES_KEY] = {
            CONSTANTS.FEE_MAKER_KEY: maker_fee_percents,
            CONSTANTS.FEE_TAKER_KEY: taker_fee_percents,
        }

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        basePrecision = chainring_utils.symbol_precision(await self.exchange_info(), trading_pair.split("-")[0])
        market_id = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        type_str = CONSTANTS.LIMIT if order_type.is_limit_type() else CONSTANTS.MARKET
        side_str = CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL

        api_params = {
            "clientOrderId": order_id,
            "marketId": market_id,
            "type": type_str,
            "side": side_str,
            "amount": {
                "type": "fixed",
                "value": str(chainring_utils.move_point_right(amount, basePrecision))
            },
            "nonce": chainring_utils.generate_order_nonce(),
            "verifyingChainId": CONSTANTS.DEFAULT_CHAIN_IDS.get(self._domain)
        }
        if order_type.is_limit_type():
            api_params["price"] = str(price)

        signature = self._auth.sign_place_order(api_params, await self.exchange_info())
        api_params["signature"] = signature

        order_result = await self._api_post(
            path_url=CONSTANTS.ORDER_PATH_URL,
            data=api_params,
            is_auth_required=True,
            limit_id=CONSTANTS.ALL_HTTP
        )

        if order_result.get('requestStatus') in {"Rejected"}:
            raise IOError({"label": "ORDER_REJECTED", "message": "Order rejected.", "error": order_result.get("error")})
        exchange_order_id = str(order_result["orderId"])

        return exchange_order_id, self._time_synchronizer.time()

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        precision = chainring_utils.symbol_precision(await self.exchange_info(), tracked_order.base_asset)
        market_id = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)

        api_params = {
            "marketId": market_id,
            "orderId": tracked_order.exchange_order_id,
            "side": CONSTANTS.SIDE_BUY if tracked_order.trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL,
            "amount": str(chainring_utils.move_point_right(tracked_order.amount, precision)),
            "nonce": chainring_utils.generate_order_nonce(),
            "verifyingChainId": CONSTANTS.DEFAULT_CHAIN_IDS.get(self._domain)
        }
        signature = self._auth.sign_place_cancel(api_params, await self.exchange_info())
        api_params["signature"] = signature

        try:
            await self._api_delete(
                path_url=f"{CONSTANTS.ORDER_PATH_URL}/{tracked_order.exchange_order_id}",
                data=api_params,
                is_auth_required=True,
                limit_id=CONSTANTS.ALL_HTTP
            )
        except Exception as e:
            if isinstance(e, ContentTypeError):
                #  aiohttp seems not able to handle 204 NoContent response
                return True
            # if self._is_order_not_found_during_cancelation_error(e):
            #     return True
            raise

        return True

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        order = await self._api_get(
            path_url=f"{CONSTANTS.ORDER_PATH_URL}/external:{tracked_order.client_order_id}",
            is_auth_required=True,
            limit_id=CONSTANTS.ALL_HTTP)

        return await self.to_order_update(order, tracked_order)

    @staticmethod
    async def to_order_update(order_update, tracked_order):
        current_state = order_update["status"]
        _order_update: OrderUpdate = OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=order_update["timing"]["closedAt"] or order_update["timing"]["updatedAt"] or order_update["timing"]["createdAt"],
            new_state=CONSTANTS.ORDER_STATE[current_state],
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=order_update["id"],
        )
        return _order_update

    async def _user_stream_event_listener(self):
        """
        This functions runs in background continuously processing the events received from the exchange by the user
        stream data source. It keeps reading events from the queue until the task is interrupted.
        The events received are balance updates, order updates and trade events.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                channel = event_message["topic"]["type"]

                if channel == "MyTrades":
                    # MyTradesCreated - new executions
                    # MyTradesUpdated - on-chain settlement updates.
                    # MyTrades - initial chunk when establishing connection
                    if event_message["data"]["type"] == "MyTradesCreated":
                        for trade in event_message["data"]["trades"]:
                            tracked_order = self._order_tracker.all_fillable_orders_by_exchange_order_id.get(trade["orderId"])
                            if tracked_order is not None:
                                trade_update = await self._to_trade_update(tracked_order, trade)
                                self._order_tracker.process_trade_update(trade_update)

                elif channel == "MyOrders":
                    if event_message["data"]["type"] == "MyOrderCreated" or event_message["data"]["type"] == "MyOrderUpdated":
                        order = event_message["data"]["order"]
                        tracked_order = self._order_tracker.all_fillable_orders_by_exchange_order_id.get(order["id"])
                        if tracked_order is not None:
                            order_update = await self.to_order_update(order, tracked_order)
                            self._order_tracker.process_order_update(order_update=order_update)
                    elif event_message["data"]["type"] == "MyOrders":
                        for order in event_message["data"]["orders"]:
                            tracked_order = self._order_tracker.all_fillable_orders_by_exchange_order_id.get(order["id"])
                            if tracked_order is not None:
                                order_update = await self.to_order_update(order, tracked_order)
                                self._order_tracker.process_order_update(order_update=order_update)

                elif channel == "Balances":
                    if event_message["data"]["type"] == "Balances":
                        balance_info = event_message["data"]
                        await self._update_total_balances_with_remote_info(balance_info)

                elif channel == "Limits":
                    if event_message["data"]["type"] == "Limits":
                        limits_info = event_message["data"]
                        await self._update_available_balances_with_remote_info(limits_info)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _to_trade_update(self, tracked_order, trade):
        trading_pair = tracked_order.trading_pair
        baseSymbol = trading_pair.split("-")[0]
        quoteSymbol = trading_pair.split("-")[1]
        basePrecision = chainring_utils.symbol_precision(await self.exchange_info(), baseSymbol)
        quotePrecision = chainring_utils.symbol_precision(await self.exchange_info(), quoteSymbol)

        amount = chainring_utils.move_point_left(Decimal(trade["amount"]), basePrecision)
        price = Decimal(trade["price"])

        feeSymbol = quoteSymbol if quoteSymbol == trade["feeSymbol"] else baseSymbol
        feeSymbolPrecision = basePrecision if quoteSymbol == trade["feeSymbol"] else quotePrecision
        feeAmount = chainring_utils.move_point_left(Decimal(trade["feeAmount"]), feeSymbolPrecision)
        if tracked_order.trade_type == TradeType.BUY:
            fee = AddedToCostTradeFee(percent_token=feeSymbol, flat_fees=[TokenAmount(feeSymbol, feeAmount)])
        else:
            fee = DeductedFromReturnsTradeFee(percent_token=feeSymbol, flat_fees=[TokenAmount(feeSymbol, feeAmount)])

        fill_datetime = datetime.fromisoformat(trade["timestamp"].replace("Z", "+00:00"))
        trade_update = TradeUpdate(
            trade_id=trade["id"],
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=tracked_order.exchange_order_id,
            trading_pair=tracked_order.trading_pair,
            is_taker=True if trade["executionRole"] == "Taker" else False,
            fee=fee,
            fill_base_amount=amount,
            fill_quote_amount=amount * price,
            fill_price=price,
            fill_timestamp=fill_datetime.timestamp(),
        )
        return trade_update

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        exchange_order = await self._api_get(
            path_url=f"{CONSTANTS.ORDER_PATH_URL}/external:{order.client_order_id}",
            is_auth_required=True,
            limit_id=CONSTANTS.ALL_HTTP)
        executions = exchange_order["executions"]

        trade_updates = []
        for execution in executions:
            # update fields to match the trade created ws message
            execution["executionRole"] = execution["role"]
            execution["id"] = execution["tradeId"]

            trade_update = await self._to_trade_update(order, execution)
            trade_updates.append(trade_update)

        return trade_updates

    async def exchange_info(self) -> Dict[str, Any]:
        if not self.trading_pair_symbol_map_ready():
            await self.trading_pair_symbol_map()

        return self._exchange_info

    async def _update_balances(self):
        balance_info = await self._api_get(
            path_url=CONSTANTS.BALANCES_PATH_URL,
            is_auth_required=True,
            limit_id=CONSTANTS.ALL_HTTP
        )
        limit_info = await self._api_get(
            path_url=CONSTANTS.LIMITS_PATH_URL,
            is_auth_required=True,
            limit_id=CONSTANTS.ALL_HTTP
        )

        await self._update_total_balances_with_remote_info(balance_info)
        await self._update_available_balances_with_remote_info(limit_info)

    async def _update_total_balances_with_remote_info(self, balance_info: Dict[str, Any]):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()
        balances = balance_info["balances"]
        for balance_entry in balances:
            asset_name = balance_entry["symbol"]
            precision = chainring_utils.symbol_precision(await self.exchange_info(), asset_name)

            total_balance = chainring_utils.move_point_left(Decimal(balance_entry["total"]), precision)
            self._account_balances[asset_name] = total_balance
            remote_asset_names.add(asset_name)
        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_balances[asset_name]
            del self._account_available_balances[asset_name]

    async def _update_available_balances_with_remote_info(self, limit_info: Dict[str, Any]):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()
        available_balances = chainring_utils.convert_limits_to_available_balances(limit_info)

        for asset_name in self._account_balances.keys():
            available_balance = available_balances[asset_name]
            precision = chainring_utils.symbol_precision(await self.exchange_info(), asset_name)
            available_balance = chainring_utils.move_point_left(Decimal(available_balance), precision)
            self._account_available_balances[asset_name] = available_balance
            remote_asset_names.add(asset_name)
        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        market_id = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        resp_json = await self._api_get(
            path_url=CONSTANTS.CONFIG_PATH_URL,
            limit_id=CONSTANTS.ALL_HTTP
        )

        market = next(market for market in resp_json["markets"] if market["id"] == market_id)
        return float(market["lastPrice"])
