import uuid
from decimal import Decimal
from typing import Any, Dict

from pydantic import Field, SecretStr

import hummingbot.connector.exchange.chainring.chainring_constants as CONSTANTS
from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.core.data_type.trade_fee import TradeFeeSchema


class ChainringConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="chainring", const=True, client_data=None)
    chainring_secret_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your wallet private key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )

    class Config:
        title = "chainring"


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information
    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise

    Example:
        {   "chains": [
                {
                    "id": ...,
                    "name": "...",
                    "contracts": [
                        ...
                    ],
                    "symbols": [
                        ...
                    ]
                }
            ],
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
            ],
            "feeRates": {
                "maker": 100,
                "taker": 200
            }
        ]
    """
    if not exchange_info.get("chains"):
        return False
    if not exchange_info.get("markets"):
        return False
    if not exchange_info.get("feeRates"):
        return False

    return True


def symbol_precision(exchange_info: Dict[str, Any], symbol: str) -> int:
    chain_id = symbol.split(":")[1]

    for chain in exchange_info['chains']:
        if str(chain['id']) == chain_id:
            for sym in chain['symbols']:
                if sym['name'] == symbol:
                    return sym['decimals']

    raise ValueError(f"Symbol {symbol} not found in the configuration.")


def chain_exchange_contract_address(exchange_info: Dict[str, Any], chain_id: int) -> str:
    for chain in exchange_info['chains']:
        if chain['id'] == chain_id:
            for contract in chain['contracts']:
                if contract['name'] == "Exchange":
                    return contract['address']

    raise ValueError(f"Exchange contract address not found for chain {chain_id} not found in the configuration.")


def token_address(exchange_info: Dict[str, Any], chain_id: int, symbol: str):
    for chain in exchange_info['chains']:
        if chain['id'] == chain_id:
            for sym in chain['symbols']:
                if sym['name'] == symbol:
                    return sym['contractAddress']

    raise ValueError(f"Contract address for symbol {symbol} on chain {chain_id} not found in the configuration.")


def convert_limits_to_available_balances(limits_response: Dict[str, Any]):
    limits = limits_response["limits"]
    limit_map = {}
    for limit in limits:
        if isinstance(limit, list):
            market_id, base_limit, quote_limit = limit
        else:
            market_id = limit['marketId']
            base_limit = limit['base']
            quote_limit = limit['quote']

        base_limit = int(base_limit)
        quote_limit = int(quote_limit)
        base_symbol, quote_symbol = market_id.split('/')

        # Update base symbol limit
        if base_symbol not in limit_map or base_limit < limit_map[base_symbol]:
            limit_map[base_symbol] = base_limit

        # Update quote symbol limit
        if quote_symbol not in limit_map or quote_limit < limit_map[quote_symbol]:
            limit_map[quote_symbol] = quote_limit

    return limit_map


def move_point_left(value: Decimal, n: int) -> Decimal:
    return value.scaleb(-n)


def move_point_right(value: Decimal, n: int) -> Decimal:
    return value.scaleb(n)


def generate_order_nonce() -> str:
    return str(uuid.uuid4()).replace('-', '')


# accessed by AllConnectorSettings in (hummingbot/settings.py)
EXAMPLE_PAIRS = {
    "localhost": "BTC:1337-ETH:1337",
    "demo": "BTC:31338-ETH:31338"
}
EXAMPLE_PAIR = EXAMPLE_PAIRS.get(CONSTANTS.DEFAULT_DOMAIN)
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0001"),
    taker_percent_fee_decimal=Decimal("0.0002"),
)
KEYS = ChainringConfigMap.construct()

OTHER_DOMAINS = []
