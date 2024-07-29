import time
from typing import Optional

import hummingbot.connector.exchange.chainring.chainring_constants as CONSTANTS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest
from hummingbot.core.web_assistant.rest_pre_processors import RESTPreProcessorBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class ChainringPerpetualRESTPreProcessor(RESTPreProcessorBase):

    async def pre_process(self, request: RESTRequest) -> RESTRequest:
        if request.headers is None:
            request.headers = {}
        request.headers["Content-Type"] = (
            "application/json"
        )
        return request


def wss_url(auth_token: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided public REST endpoint
    :param auth_token: signed token for ws authentication
    :param domain: the ChainRing net configuration
    :return: the full URL to the endpoint
    """
    return CONSTANTS.WSS_URLS.get(domain).format(domain) + "?auth=" + auth_token


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided public REST endpoint
    :param path_url: a public REST endpoint
    :param domain: the ChainRing net configuration
    :return: the full URL to the endpoint
    """
    return CONSTANTS.REST_URLS.get(domain).format(domain) + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided private REST endpoint
    :param path_url: a private REST endpoint
    :param domain: the ChainRing net configuration
    :return: the full URL to the endpoint
    """
    return CONSTANTS.REST_URLS.get(domain).format(domain) + path_url


def build_api_factory(
        throttler: Optional[AsyncThrottler],
        auth: Optional[AuthBase]) -> WebAssistantsFactory:
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[
            ChainringPerpetualRESTPreProcessor(),  # adds Content-Type "application/json"
        ]
    )
    return api_factory


async def get_current_server_time(
        throttler: AsyncThrottler,
        domain: str
) -> float:
    return time.time()
