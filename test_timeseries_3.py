import datetime as dt
import logging
import pytest
import pandas as pd # Importar pandas
from pathlib import Path # Importar Path para manejar rutas de archivos

from teii.finance import FinanceClientParamError
from teii.finance.timeseries import TimeSeriesFinanceClient


MSFT_VALID_DATA = {
    "Meta Data": {
        "1. Information": "Weekly Adjusted Prices and Volumes",
        "2. Symbol": "MSFT",
        "3. Last Refreshed": "2023-10-27",
        "4. Time Zone": "US/Eastern"
    },
    "Weekly Adjusted Time Series": {
        "2023-10-27": {
            "1. open": "332.00",
            "2. high": "340.00",
            "3. low": "330.00",
            "4. close": "338.00",
            "5. adjusted close": "338.00",
            "6. volume": "1000000",
            "7. dividend amount": "0.00"
        },
        "2023-10-20": {
            "1. open": "320.00",
            "2. high": "330.00",
            "3. low": "318.00",
            "4. close": "325.00",
            "5. adjusted close": "325.00",
            "6. volume": "1200000",
            "7. dividend amount": "0.00"
        },
        "2023-10-13": {
            "1. open": "310.00",
            "2. high": "320.00",
            "3. low": "308.00",
            "4. close": "315.00",
            "5. adjusted close": "315.00",
            "6. volume": "900000",
            "7. dividend amount": "0.00"
        },
        "2023-10-06": {
            "1. open": "300.00",
            "2. high": "310.00",
            "3. low": "298.00",
            "4. close": "305.00",
            "5. adjusted close": "305.00",
            "6. volume": "1100000",
            "7. dividend amount": "0.00"
        }
    }
}

# Datos simulados para IBM, incluyendo el volumen
IBM_VALID_DATA = {
    "Meta Data": {
        "1. Information": "Weekly Adjusted Prices and Volumes",
        "2. Symbol": "IBM",
        "3. Last Refreshed": "2023-10-27",
        "4. Time Zone": "US/Eastern"
    },
    "Weekly Adjusted Time Series": {
        "2023-10-27": {
            "1. open": "145.00", "2. high": "148.00", "3. low": "144.00",
            "4. close": "147.00", "5. adjusted close": "147.00",
            "6. volume": "500000", "7. dividend amount": "0.00"
        },
        "2023-10-20": {
            "1. open": "140.00", "2. high": "145.00", "3. low": "139.00",
            "4. close": "142.00", "5. adjusted close": "142.00",
            "6. volume": "600000", "7. dividend amount": "0.00"
        },
        "2023-10-13": {
            "1. open": "138.00", "2. high": "142.00", "3. low": "137.00",
            "4. close": "140.00", "5. adjusted close": "140.00",
            "6. volume": "700000", "7. dividend amount": "0.00"
        },
        "2023-10-06": {
            "1. open": "135.00", "2. high": "139.00", "3. low": "134.00",
            "4. close": "137.00", "5. adjusted close": "137.00",
            "6. volume": "800000", "7. dividend amount": "0.00"
        },
        "2023-09-29": {
            "1. open": "130.00", "2. high": "135.00", "3. low": "129.00",
            "4. close": "132.00", "5. adjusted close": "132.00",
            "6. volume": "900000", "7. dividend amount": "0.00"
        }
    }
}


@pytest.fixture
def mocked_requests(requests_mock):
    requests_mock.get(
        "https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol=MSFT&outputsize=full&apikey=TEST",
        json=MSFT_VALID_DATA
    )
    requests_mock.get(
        "https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol=NODATA&outputsize=full&apikey=TEST",
        json={
            "Meta Data": {"2. Symbol": "NODATA", "1. Information": "Weekly Adjusted Prices and Volumes"},
        }
    )
    # Añadir mock para IBM
    requests_mock.get(
        "https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol=IBM&outputsize=full&apikey=TEST",
        json=IBM_VALID_DATA
    )
    return requests_mock


# Ruta base para los archivos de datos de referencia (ajusta según tu estructura de carpetas)
TEST_DATA_PATH = Path(__file__).parent / "data"

class TestTimeSeriesFinanceClient:

    def test_weekly_price_invalid_dates(self, mocked_requests):
        """
        Test that FinanceClientParamError is raised when from_date is greater than to_date
        in weekly_price method.
        """
        client = TimeSeriesFinanceClient("MSFT", api_key="TEST", logging_level=logging.CRITICAL)

        future_date = dt.date(2024, 1, 1)
        past_date = dt.date(2023, 1, 1)

        with pytest.raises(FinanceClientParamError) as excinfo:
            client.weekly_price(from_date=future_date, to_date=past_date)

        assert "'from_date' cannot be greater than 'to_date'" in str(excinfo.value)

    def test_weekly_volume_invalid_dates(self, mocked_requests):
        """
        Test that FinanceClientParamError is raised when from_date is greater than to_date
        in weekly_volume method.
        """
        client = TimeSeriesFinanceClient("MSFT", api_key="TEST", logging_level=logging.CRITICAL)

        future_date = dt.date(2024, 1, 1)
        past_date = dt.date(2023, 1, 1)

        with pytest.raises(FinanceClientParamError) as excinfo:
            client.weekly_volume(from_date=future_date, to_date=past_date)

        assert "'from_date' cannot be greater than 'to_date'" in str(excinfo.value)

    def test_weekly_volume_unfiltered(self, mocked_requests):
        """
        Test weekly_volume() method returns the correct unfiltered volume series
        matching the reference CSV.
        """
        client = TimeSeriesFinanceClient("IBM", api_key="TEST", logging_level=logging.CRITICAL)
        
        # Cargar la serie de referencia desde el CSV
        expected_series = pd.read_csv(
            TEST_DATA_PATH / "TIME_SERIES_WEEKLY_ADJUSTED.IBM.volume.unfiltered.csv",
            index_col='date',
            parse_dates=True
        )['volume'] # Seleccionar solo la columna 'volume'

        actual_series = client.weekly_volume()

        # Asegurarse de que los índices son iguales y los valores son iguales
        pd.testing.assert_series_equal(actual_series, expected_series)

    def test_weekly_volume_filtered(self, mocked_requests):
        """
        Test weekly_volume() method returns the correct filtered volume series
        matching the reference CSV for a specific date range.
        """
        client = TimeSeriesFinanceClient("IBM", api_key="TEST", logging_level=logging.CRITICAL)

        # Definir el rango de fechas para el filtro
        from_date = dt.date(2023, 10, 13)
        to_date = dt.date(2023, 10, 27)
        
        # Cargar la serie de referencia desde el CSV filtrado
        expected_series = pd.read_csv(
            TEST_DATA_PATH / "TIME_SERIES_WEEKLY_ADJUSTED.IBM.volume.filtered.csv",
            index_col='date',
            parse_dates=True
        )['volume'] # Seleccionar solo la columna 'volume'

        actual_series = client.weekly_volume(from_date=from_date, to_date=to_date)

        # Asegurarse de que los índices son iguales y los valores son iguales
        pd.testing.assert_series_equal(actual_series, expected_series)
