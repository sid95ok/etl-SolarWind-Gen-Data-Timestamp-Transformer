""" API integration test - Backend API should be accessible to run these tests,
    with proper environment variables set i.e. API_KEY and API_SERVER_URL
"""
import unittest
from commons.util import request_api_call


class APIIntegrationTests(unittest.TestCase):

    def setUp(self):
        pass

    def test_request_api_call_status(self):
        response = request_api_call()
        self.assertDictEqual({'status': 'ok'}, response)

    def test_request_api_call_solar(self):
        response = request_api_call(f"/2024-01-01/renewables/solargen.json")
        self.assertIsNotNone(response)
        self.assertIsInstance(response, list)

    def test_request_api_call_wind(self):
        response = request_api_call(f"/2024-01-01/renewables/windgen.csv")
        self.assertIsNotNone(response)
        self.assertIsInstance(response, str)


if __name__ == '__main__':
    unittest.main()
