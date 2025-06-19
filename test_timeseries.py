import logging
import pytest

from teii.finance import FinanceClientInvalidData
from teii.finance.timeseries import TimeSeriesFinanceClient

# Asume que tu sistema de mockeo de requests está configurado,
# por ejemplo, en un fixture 'mocked_requests' en conftest.py o directamente aquí.

# Contenido del archivo NODATA.json simulado para el test
NODATA_JSON_CONTENT = {
    "Meta Data": {
        "1. Information": "Weekly Adjusted Prices and Volumes",
        "2. Symbol": "NODATA",
        "3. Last Refreshed": "2023-10-27",
        "4. Time Zone": "US/Eastern"
    }
}

# Si tu fixture mocked_requests necesita esta configuración directa:
# @pytest.fixture
# def mocked_requests(requests_mock):
#     requests_mock.get(
#         "https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol=NODATA&outputsize=full&apikey=TEST",
#         json=NODATA_JSON_CONTENT
#     )
#     # Añade aquí otros mocks necesarios para el resto de tus tests
#     return requests_mock

class TestTimeSeriesFinanceClient:

    def test_constructor_invalid_data(self, mocked_requests):
        """
        Test that FinanceClientInvalidData is raised when the API response
        does not contain weekly adjusted time series data.
        """
        with pytest.raises(FinanceClientInvalidData) as excinfo:
            TimeSeriesFinanceClient("NODATA", api_key="TEST", logging_level=logging.CRITICAL)

        assert "No weekly adjusted time series data found in API response." in str(excinfo.value)
