from unittest import TestCase

from hummingbot.connector.exchange.chainring.chainring_order_book import ChainringOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class ChainringOrderBookTests(TestCase):

    def test_snapshot_message_from_exchange(self):
        snapshot_message = ChainringOrderBook.snapshot_message_from_exchange(
            msg={
                "type": "OrderBook",
                "marketId": "BTC:31338/BTC:31339",
                "buy": [
                    {
                        "price": "1.0130",
                        "size": "2.725756997539267978"
                    }
                ],
                "sell": [
                    {
                        "price": "1.0460",
                        "size": "2.5715033082544563"
                    }
                ],
                "last": {
                    "price": "1.0130",
                    "direction": "Unchanged"
                }
            },
            timestamp=1640000000.0,
            metadata={"trading_pair": "BTC:31338-BTC:31339"}
        )

        self.assertEqual("BTC:31338-BTC:31339", snapshot_message.trading_pair)
        self.assertEqual(OrderBookMessageType.SNAPSHOT, snapshot_message.type)
        self.assertEqual(1640000000.0, snapshot_message.timestamp)
        # timestamp is used as update_id if not provided explicitly
        self.assertEqual(1640000000.0, snapshot_message.update_id)
        self.assertEqual(-1, snapshot_message.trade_id)

        self.assertEqual(1, len(snapshot_message.bids))
        self.assertEqual(1.013, snapshot_message.bids[0].price)
        self.assertEqual(2.725756997539267978, snapshot_message.bids[0].amount)
        self.assertEqual(1640000000.0, snapshot_message.bids[0].update_id)

        self.assertEqual(1, len(snapshot_message.asks))
        self.assertEqual(1.0460, snapshot_message.asks[0].price)
        self.assertEqual(2.5715033082544563, snapshot_message.asks[0].amount)
        self.assertEqual(1640000000.0, snapshot_message.asks[0].update_id)
