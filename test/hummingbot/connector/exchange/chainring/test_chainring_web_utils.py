import unittest

import hummingbot.connector.exchange.chainring.chainring_constants as CONSTANTS
from hummingbot.connector.exchange.chainring import chainring_web_utils as web_utils


class ChainringWebUtilTestCases(unittest.TestCase):

    def test_wss_url(self):
        domain = "demo"
        expected_url = CONSTANTS.WSS_URLS.get(domain) + '?auth=token'
        self.assertEqual(expected_url, web_utils.wss_url("token", domain))

    def test_public_rest_url(self):
        path_url = "/TEST_PATH"
        domain = "demo"
        expected_url = CONSTANTS.REST_URLS.get(domain) + path_url
        self.assertEqual(expected_url, web_utils.public_rest_url(path_url, domain))

    def test_private_rest_url(self):
        path_url = "/TEST_PATH"
        domain = "demo"
        expected_url = CONSTANTS.REST_URLS.get(domain) + path_url
        self.assertEqual(expected_url, web_utils.private_rest_url(path_url, domain))
