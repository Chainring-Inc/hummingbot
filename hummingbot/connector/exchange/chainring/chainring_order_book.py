import time
from decimal import Decimal
from typing import Dict, Optional

import hummingbot.connector.exchange.chainring.chainring_constants as CONSTANTS
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class ChainringOrderBook(OrderBook):

    @classmethod
    def trade_message_from_exchange(cls,
                                    trading_pair: str,
                                    trade: [any]) -> OrderBookMessage:
        """
        Creates a trade messagee
        :param msg: the response from the exchange when requesting the order book snapshot
        :param timestamp: the snapshot timestamp
        :param metadata: a dictionary with extra information to add to the snapshot data
        :return: a snapshot message with the snapshot information received from the exchange
        """
        return OrderBookMessage(OrderBookMessageType.TRADE, {
            "trade_id": trade[0],
            "trade_type": float(TradeType.SELL.value) if trade[1] == CONSTANTS.SIDE_SELL else float(TradeType.BUY.value),
            "amount": Decimal(trade[2]),
            "price": Decimal(trade[3]),
            "trading_pair": trading_pair,
            "update_id": time.time(),
        }, timestamp=trade[4])

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Creates a snapshot message with the order book snapshot message
        :param msg: the response from the exchange when requesting the order book snapshot
        :param timestamp: the snapshot timestamp
        :param metadata: a dictionary with extra information to add to the snapshot data
        :return: a snapshot message with the snapshot information received from the exchange
        """
        if metadata:
            msg.update(metadata)
        ts = timestamp
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["trading_pair"],
            "update_id": ts,
            "bids": [[Decimal(i['price']), Decimal(i['size'])] for i in msg["buy"]],
            "asks": [[Decimal(i['price']), Decimal(i['size'])] for i in msg["sell"]],
        }, timestamp=ts)
