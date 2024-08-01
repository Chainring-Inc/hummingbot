import base64
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict

import eth_account
from eth_account.messages import encode_structured_data

import hummingbot.connector.exchange.chainring.chainring_constants as CONSTANTS
from hummingbot.connector.exchange.chainring import chainring_utils
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class ChainringAuth(AuthBase):
    def __init__(self, secret_key: str, domain: str):
        self._secret_key = secret_key
        self._domain = domain
        self._wallet = eth_account.Account.from_key(secret_key)

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds authorization token to all requests
        :param request: the request to be configured for authenticated interaction
        """
        headers = {}
        if request.headers is not None:
            headers.update(request.headers)
        auth_token = self.sign_login_action()

        headers.update(self.header_for_authentication(auth_token))
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # web socket authorization is performed during establishing connection
        return request

    @staticmethod
    def header_for_authentication(token: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {token}"}

    def sign_login_action(self):
        message = {
            'message': '[ChainRing Labs] Please sign this message to verify your ownership of this wallet address. This action will not cost any gas fees.',
            'address': self._wallet.address.lower(),
            'chainId': CONSTANTS.DEFAULT_CHAIN_IDS.get(self._domain),
            'timestamp': datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        }

        data = {
            "domain": {
                "chainId": CONSTANTS.DEFAULT_CHAIN_IDS.get(self._domain),
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
        signature = self._wallet.sign_message(structured_data)
        sign_in_message_body = base64.urlsafe_b64encode(json.dumps(message).encode()).decode()

        # FIXME cache and re-use token
        return f"{sign_in_message_body}.{signature['signature'].hex()}"

    def sign_place_order(self, api_params: Dict[str, Any], exchange_info: Dict[str, Any]):
        market_id = api_params["marketId"]
        base_symbol = market_id.split("/")[0]
        quote_symbol = market_id.split("/")[1]

        base_chain_id = int(base_symbol.split(":")[1])
        base_token = chainring_utils.token_address(exchange_info, base_chain_id, base_symbol) or CONSTANTS.ADDRESS_ZERO

        quote_chain_id = int(quote_symbol.split(":")[1])
        quote_token = chainring_utils.token_address(exchange_info, quote_chain_id, quote_symbol) or CONSTANTS.ADDRESS_ZERO

        amount = int(Decimal(api_params["amount"]["value"]))
        quote_precision = chainring_utils.symbol_precision(exchange_info, quote_symbol)
        price = '0' if api_params["type"] == CONSTANTS.MARKET else chainring_utils.move_point_right(Decimal(api_params["price"]), quote_precision)

        exchange_contract_address = chainring_utils.chain_exchange_contract_address(exchange_info, base_chain_id)

        data = {
            "domain": self.domain(base_chain_id, exchange_contract_address),
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
                "sender": self._wallet.address,
                "baseChainId": base_chain_id,
                "baseToken": base_token,
                "quoteChainId": quote_chain_id,
                "quoteToken": quote_token,
                "amount": amount if api_params["side"] == CONSTANTS.SIDE_BUY else -amount,
                "price": int(price),
                "nonce": int(api_params["nonce"], 16),
            },
        }

        structured_data = encode_structured_data(data)
        signature = self._wallet.sign_message(structured_data)

        return signature['signature'].hex()

    def sign_place_cancel(self, api_params: Dict[str, Any], exchange_info: Dict[str, Any]) -> str:
        market_id = api_params["marketId"]
        base_symbol = market_id.split("/")[0]
        base_chain_id = int(base_symbol.split(":")[1])

        exchange_contract_address = chainring_utils.chain_exchange_contract_address(exchange_info, base_chain_id)

        amount = int(Decimal(api_params["amount"]))
        data = {
            "domain": self.domain(base_chain_id, exchange_contract_address),
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
                "sender": self._wallet.address,
                "marketId": api_params["marketId"],
                "amount": amount if api_params["side"] == CONSTANTS.SIDE_BUY else -amount,
                "nonce": int(api_params["nonce"], 16),
            },
        }

        structured_data = encode_structured_data(data)
        signature = self._wallet.sign_message(structured_data)

        return signature['signature'].hex()

    @staticmethod
    def domain(chain_id: int, exchange_contract_address: str):
        return {
            'name': 'ChainRing Labs',
            'chainId': chain_id,
            'verifyingContract': exchange_contract_address,
            'version': '0.0.1'
        }
