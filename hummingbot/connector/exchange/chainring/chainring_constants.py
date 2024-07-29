import sys

from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

WS_HEARTBEAT_TIME_INTERVAL = 30

DEFAULT_DOMAIN = "localhost"

# Chains
DEFAULT_CHAIN_IDS = {
    "demo": 31338,
    "localhost": 1337
}

# Base URL
REST_URLS = {
    "demo": "https://demo-api.chainring.co",
    "localhost": "http://localhost:9000"
}
WSS_URLS = {
    "demo": "wss://demo-api.chainring.co/connect",
    "localhost": "ws://localhost:9000/connect"
}


# API Endpoints
CONFIG_PATH_URL = "/v1/config"
BALANCES_PATH_URL = "/v1/balances"
LIMITS_PATH_URL = "/v1/limits"
ORDER_PATH_URL = "/v1/orders"
ORDER_BOOK_PATH_URL = "/v1/order-book/{}"


# Fee
FEES_KEY = "*"
FEE_MAKER_KEY = "maker"
FEE_TAKER_KEY = "taker"


# Order
LIMIT = "limit"
MARKET = "market"
SIDE_BUY = "Buy"
SIDE_SELL = "Sell"
ADDRESS_ZERO = "0x0000000000000000000000000000000000000000"
ORDER_STATE = {
    "Open": OrderState.OPEN,
    "Filled": OrderState.FILLED,
    "Partial": OrderState.PARTIALLY_FILLED,
    "Cancelled": OrderState.CANCELED,
    "Expired": OrderState.CANCELED,
    "Rejected": OrderState.CANCELED,
    "Failed": OrderState.FAILED,
}

ERROR_CODE_REJECTED_BY_SEQUENCER = "RejectedBySequencer"
ERROR_CODE_ORDER_NOT_FOUND = "OrderNotFound"


# Rate Limits
# NOTE: ChainRing has no api limits in place atm
ALL_HTTP = "ALL_HTTP"
NO_LIMIT = sys.maxsize
RATE_LIMITS = [
    RateLimit(limit_id=CONFIG_PATH_URL, limit=NO_LIMIT, time_interval=1),
    RateLimit(limit_id=ALL_HTTP, limit=NO_LIMIT, time_interval=1),
]
