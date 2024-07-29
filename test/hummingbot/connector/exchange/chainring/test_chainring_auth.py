import asyncio
import base64
import json
from typing import Any, Dict
from unittest import TestCase

from eth_account import Account
from eth_account.messages import encode_structured_data
from typing_extensions import Awaitable

import hummingbot.connector.exchange.chainring.chainring_constants as CONSTANTS
from hummingbot.connector.exchange.chainring.chainring_auth import ChainringAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class ChainringAuthTests(TestCase):

    def setUp(self) -> None:
        self.wallet_address = "0x8Db2c249F8E33aCb8bD1b7De71d5e7079f541E71"
        self.wallet_private_key = "0xce5715be4e423b41bb3e62bac046f9dc99041c7af3e49492e0f1b44a15de5c4b"  # noqa: mock

        self.auth = ChainringAuth(
            secret_key=self.wallet_private_key,
            domain="demo",
        )

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
                }
            ],
            "markets": [
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
                }
            ],
            "feeRates": {
                "maker": 100,
                "taker": 200
            }
        }

    @staticmethod
    def async_run_with_timeout(coroutine: Awaitable, timeout: int = 1):
        ret = asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    @staticmethod
    def _get_request(params: Dict[str, Any]) -> RESTRequest:
        return RESTRequest(
            method=RESTMethod.GET,
            url="https://chainring/api/endpoint",
            is_auth_required=True,
            params=params,
        )

    def test_rest_authenticate(self):
        request = self._get_request({})
        self.async_run_with_timeout(self.auth.rest_authenticate(request))

        auth_header = request.headers.get("Authorization")
        self.assertTrue(auth_header.startswith("Bearer"))
        auth_token = auth_header.split(" ")[1]

        sign_in_message_body = auth_token.split(".")[0]
        signature = auth_token.split(".")[1]

        sign_in_message_body_decoded = base64.urlsafe_b64decode(sign_in_message_body).decode()
        message = json.loads(sign_in_message_body_decoded)

        # Recreate the structured data
        data = {
            "domain": {
                "chainId": message["chainId"],
                "name": "ChainRing Labs",
            },
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "chainId", "type": "uint32"},
                ],
                "Sign In": [
                    {"name": "message", "type": "string"},
                    {"name": "address", "type": "string"},
                    {"name": "chainId", "type": "uint32"},
                    {"name": "timestamp", "type": "string"},
                ],
            },
            "primaryType": "Sign In",
            "message": message,
        }

        structured_data = encode_structured_data(data)
        recovered_address = Account.recover_message(structured_data, signature=signature)

        self.assertEqual(recovered_address.lower(), message["address"].lower())

    def test_sign_place_order(self):
        api_params: Dict[str, Any] = {
            "marketId": "BTC:31338/ETH:31338",
            "amount": {"value": "1000"},
            "price": "0.5",
            "type": CONSTANTS.LIMIT,
            "side": CONSTANTS.SIDE_BUY,
            "nonce": "0x1"
        }

        signature = self.auth.sign_place_order(api_params, self.valid_exchange_info)

        data = {
            "domain": {
                'name': 'ChainRing Labs',
                'chainId': 31338,
                'verifyingContract': "0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512",
                'version': '0.0.1'
            },
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "Order": [
                    {"name": "sender", "type": "address"},
                    {"name": "baseChainId", "type": "uint256"},
                    {"name": "baseToken", "type": "address"},
                    {"name": "quoteChainId", "type": "uint256"},
                    {"name": "quoteToken", "type": "address"},
                    {"name": "amount", "type": "int256"},
                    {"name": "price", "type": "uint256"},
                    {"name": "nonce", "type": "int256"},
                ],
            },
            "primaryType": "Order",
            "message": {
                "sender": self.wallet_address,
                "baseChainId": 31338,
                "baseToken": CONSTANTS.ADDRESS_ZERO,
                "quoteChainId": 31338,
                "quoteToken": "0xCf7Ed3AccA5a467e9e704C703E8D87F634fB0Fc9",
                "amount": 1000,
                "price": 500000000000000000,
                "nonce": int("0x1", 16),
            },
        }

        structured_data = encode_structured_data(data)
        recovered_address = Account.recover_message(structured_data, signature=signature)

        self.assertEqual(recovered_address.lower(), self.wallet_address.lower())

    def test_sign_place_cancel(self):
        api_params: Dict[str, Any] = {
            "marketId": "BTC:31338/ETH:31338",
            "amount": "1000",
            "price": "0.5",
            "type": CONSTANTS.LIMIT,
            "side": CONSTANTS.SIDE_BUY,
            "nonce": "0x1"
        }

        signature = self.auth.sign_place_cancel(api_params, self.valid_exchange_info)

        data = {
            "domain": {
                'name': 'ChainRing Labs',
                'chainId': 31338,
                'verifyingContract': "0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512",
                'version': '0.0.1'
            },
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "CancelOrder": [
                    {"name": "sender", "type": "address"},
                    {"name": "marketId", "type": "string"},
                    {"name": "amount", "type": "int256"},
                    {"name": "nonce", "type": "int256"},
                ],
            },
            "primaryType": "CancelOrder",
            "message": {
                "sender": self.wallet_address,
                "marketId": "BTC:31338/ETH:31338",
                "amount": 1000,
                "nonce": int("0x1", 16),
            },
        }

        structured_data = encode_structured_data(data)
        recovered_address = Account.recover_message(structured_data, signature=signature)

        self.assertEqual(recovered_address.lower(), self.wallet_address.lower())

    def test_domain(self):
        self.assertEqual(
            {
                'name': 'ChainRing Labs',
                'chainId': 1,
                'verifyingContract': "0x0",
                'version': '0.0.1'
            },
            self.auth.domain(1, "0x0")
        )
