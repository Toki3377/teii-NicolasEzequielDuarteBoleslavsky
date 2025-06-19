import logging
import pytest

from teii.finance import FinanceClientInvalidData
from teii.finance.timeseries import TimeSeriesFinanceClient

# Asegúrate de que 'mocked_requests' sea accesible o esté en el mismo archivo de tests
# Si no lo tienes, deberías tener una implementación similar a esta en tu entorno de pruebas:
# from unittest.mock import patch, MagicMock

# Si 'mocked_requests' ya está definido en tu suite de tests, úsalo.
import logging
import pytest

from teii.finance import FinanceClientInvalidData
from teii.finance.timeseries import TimeSeriesFinanceClient


NODATA_JSON_CONTENT = {
    "Meta Data": {
        "1. Information": "Weekly Adjusted Prices and Volumes",
        "2. Symbol": "NODATA",
        "3. Last Refreshed": "2023-10-27",
        "4. Time Zone": "US/Eastern"
    }
}


class TestTimeSeriesFinanceClient:

    def test_constructor_invalid_data(self, mocked_requests):
        with pytest.raises(FinanceClientInvalidData) as excinfo:
            TimeSeriesFinanceClient("NODATA", api_key="TEST", logging_level=logging.CRITICAL)

        assert "No weekly adjusted time series data found in API response." in str(excinfo.value)
