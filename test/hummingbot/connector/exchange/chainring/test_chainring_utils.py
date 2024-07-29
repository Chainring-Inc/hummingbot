import unittest
from decimal import Decimal

from hummingbot.connector.exchange.chainring import chainring_utils as utils


class ChainringUtilTestCases(unittest.TestCase):

    def setUp(self) -> None:
        self.valid_exchange_info = {
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
                        },
                        {
                            "name": "ETH:31338",
                            "description": "Ethereum",
                            "contractAddress": "0xCf7Ed3AccA5a467e9e704C703E8D87F634fB0Fc9",
                            "decimals": 18,
                            "faucetSupported": True,
                            "iconUrl": "https://chainring-web-icons.s3.us-east-2.amazonaws.com/symbols/eth.svg",
                            "withdrawalFee": "300000000000000"
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
                    "lastPrice": "1.008000000000000000",
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
                    "lastPrice": "17.550000000000000000",
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
                    "lastPrice": "16.600000000000000000",
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
                {
                    "id": "BTC:31339/USDC:31339",
                    "baseSymbol": "BTC:31339",
                    "baseDecimals": 18,
                    "quoteSymbol": "USDC:31339",
                    "quoteDecimals": 6,
                    "tickSize": "25.000000000000000000",
                    "lastPrice": "60812.500000000000000000",
                    "minAllowedBidPrice": "0.000000000000000000",
                    "maxAllowedOfferPrice": "2147483647.000000000000000000",
                    "minFee": "20000"
                },
                {
                    "id": "USDC:31338/DAI:31338",
                    "baseSymbol": "USDC:31338",
                    "baseDecimals": 6,
                    "quoteSymbol": "DAI:31338",
                    "quoteDecimals": 18,
                    "tickSize": "0.010000000000000000",
                    "lastPrice": "2.005000000000000000",
                    "minAllowedBidPrice": "0.000000000000000000",
                    "maxAllowedOfferPrice": "2147483647.000000000000000000",
                    "minFee": "20000000000000000"
                },
                {
                    "id": "USDC:31339/DAI:31339",
                    "baseSymbol": "USDC:31339",
                    "baseDecimals": 6,
                    "quoteSymbol": "DAI:31339",
                    "quoteDecimals": 18,
                    "tickSize": "0.010000000000000000",
                    "lastPrice": "2.005000000000000000",
                    "minAllowedBidPrice": "0.000000000000000000",
                    "maxAllowedOfferPrice": "2147483647.000000000000000000",
                    "minFee": "20000000000000000"
                }
            ],
            "feeRates": {
                "maker": 100,
                "taker": 200
            }
        }

        self.limits_response = {
            "limits": [
                ["BTC:1337/BTC:1338", "400000000000000000000", "500000000000000000000"],
                ["BTC:1337/ETH:1337", "500000000000000000000", "0"],
                ["BTC:1337/ETH:1338", "500000000000000000000", "0"],
                ["BTC:1337/USDC:1337", "500000000000000000000", "0"],
                ["BTC:1338/ETH:1338", "500000000000000000000", "0"],
                ["BTC:1338/USDC:1338", "500000000000000000000", "0"],
                ["USDC:1337/DAI:1337", "0", "0"],
                ["USDC:1338/DAI:1338", "0", "0"]
            ]
        }

    def test_is_exchange_information_valid(self):
        self.assertTrue(utils.is_exchange_information_valid(self.valid_exchange_info))

        invalid_exchange_info_1 = {}
        self.assertFalse(utils.is_exchange_information_valid(invalid_exchange_info_1))

        invalid_exchange_info_2 = self.valid_exchange_info.copy()
        del invalid_exchange_info_2["chains"]
        self.assertFalse(utils.is_exchange_information_valid(invalid_exchange_info_2))

        invalid_exchange_info_3 = self.valid_exchange_info.copy()
        del invalid_exchange_info_3["markets"]
        self.assertFalse(utils.is_exchange_information_valid(invalid_exchange_info_3))

        invalid_exchange_info_4 = self.valid_exchange_info.copy()
        del invalid_exchange_info_4["feeRates"]
        self.assertFalse(utils.is_exchange_information_valid(invalid_exchange_info_4))

    def test_symbol_precision(self):
        self.assertEquals(18, utils.symbol_precision(self.valid_exchange_info, "BTC:31338"))
        self.assertEquals(6, utils.symbol_precision(self.valid_exchange_info, "USDC:31339"))

        with self.assertRaisesRegex(ValueError, "Symbol test:test not found in the configuration."):
            utils.symbol_precision(self.valid_exchange_info, "test:test")

    def test_chain_exchange_contract_address(self):
        self.assertEquals("0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512", utils.chain_exchange_contract_address(self.valid_exchange_info, 31338))
        self.assertEquals("0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512", utils.chain_exchange_contract_address(self.valid_exchange_info, 31339))

        with self.assertRaisesRegex(ValueError, "Exchange contract address not found for chain 111 not found in the configuration."):
            utils.chain_exchange_contract_address(self.valid_exchange_info, 111)

    def test_token_address(self):
        self.assertEquals(None, utils.token_address(self.valid_exchange_info, 31338, "BTC:31338"))
        self.assertEquals("0xCf7Ed3AccA5a467e9e704C703E8D87F634fB0Fc9", utils.token_address(self.valid_exchange_info, 31338, "ETH:31338"))

        with self.assertRaisesRegex(ValueError, "Contract address for symbol test:test on chain 31338 not found in the configuration."):
            utils.token_address(self.valid_exchange_info, 31338, "test:test")

    def test_convert_limits_to_available_balances(self):
        actual_available_balances = utils.convert_limits_to_available_balances(self.limits_response)
        expected_available_balances = {
            "BTC:1337": 400000000000000000000,
            "BTC:1338": 500000000000000000000,
            "DAI:1337": 0,
            "DAI:1338": 0,
            "ETH:1337": 0,
            "ETH:1338": 0,
            "USDC:1337": 0,
            "USDC:1338": 0
        }

        self.assertEquals(actual_available_balances, expected_available_balances)

    def test_move_point_right(self):
        self.assertEquals(Decimal("0.0001"), utils.move_point_right(Decimal("0.00001"), 1))
        self.assertEquals(Decimal("10"), utils.move_point_right(Decimal("1"), 1))
        self.assertEquals(Decimal("1000"), utils.move_point_right(Decimal("0.01"), 5))
        self.assertEquals(Decimal("1000000000000000000"), utils.move_point_right(Decimal("1"), 18))

    def test_move_point_left(self):
        self.assertEquals(Decimal("0.001"), utils.move_point_left(Decimal("0.01"), 1))
        self.assertEquals(Decimal("0.0000001"), utils.move_point_left(Decimal("0.01"), 5))
        self.assertEquals(Decimal("1"), utils.move_point_left(Decimal("1000000000000000000"), 18))

    def test_generate_order_nonce(self):
        self.assertEquals(32, len(utils.generate_order_nonce()))
