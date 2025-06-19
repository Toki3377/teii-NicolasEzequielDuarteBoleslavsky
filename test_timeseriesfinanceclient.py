""" Unit tests for teii.finance.timeseries module """


import datetime as dt

import pytest
import requests # Importar requests para la excepción ConnectionError
from pandas.testing import assert_series_equal

from teii.finance import FinanceClientInvalidAPIKey, FinanceClientParamError, FinanceClientAPIError, TimeSeriesFinanceClient


# ... (MSFT_VALID_DATA, IBM_VALID_DATA, y NODATA_JSON_CONTENT de tus archivos existentes) ...

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
    requests_mock.get(
        "https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol=IBM&outputsize=full&apikey=TEST",
        json=IBM_VALID_DATA
    )
    # Mock para el nuevo caso de error de conexión
    requests_mock.get(
        "https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol=NOCONNECT&outputsize=full&apikey=TEST",
        exc=requests.exceptions.ConnectionError # Configura el mock para lanzar esta excepción
    )
    return requests_mock


# ... (TEST_DATA_PATH de tus archivos existentes) ...

class TestTimeSeriesFinanceClient:

    def test_constructor_success(self, api_key_str,
                                 mocked_requests):
        TimeSeriesFinanceClient("IBM", api_key_str)


    def test_constructor_failure_invalid_api_key(self):
        with pytest.raises(FinanceClientInvalidAPIKey):
            TimeSeriesFinanceClient("IBM")


    def test_weekly_price_invalid_dates(self, api_key_str,
                                        mocked_requests):
        fc = TimeSeriesFinanceClient("IBM", api_key_str)
        with pytest.raises(FinanceClientParamError):
            fc.weekly_price(dt.date(year=2024, month=1, day=1),
                            dt.date(year=2023, month=1, day=1))


    def test_weekly_price_no_dates(self, api_key_str,
                                   mocked_requests,
                                   pandas_series_IBM_prices):
        fc = TimeSeriesFinanceClient("IBM", api_key_str)

        ps = fc.weekly_price()

        assert ps.count() == 1276

        assert ps.count() == pandas_series_IBM_prices.count()

        assert_series_equal(ps, pandas_series_IBM_prices)


    def test_weekly_price_dates(self, api_key_str,
                                mocked_requests,
                                pandas_series_IBM_prices_filtered):
        fc = TimeSeriesFinanceClient("IBM", api_key_str)

        ps = fc.weekly_price(dt.date(year=2021, month=1, day=1),
                             dt.date(year=2023, month=12, day=31))

        assert ps.count() == 156

        assert ps.count() == pandas_series_IBM_prices_filtered.count()

        assert_series_equal(ps, pandas_series_IBM_prices_filtered)


    def test_weekly_volume_invalid_dates(self, api_key_str,
                                         mocked_requests):
        fc = TimeSeriesFinanceClient("IBM", api_key_str)
        with pytest.raises(FinanceClientParamError):
            fc.weekly_volume(dt.date(year=2024, month=1, day=1),
                             dt.date(year=2023, month=1, day=1))


    def test_weekly_volume_no_dates(self, api_key_str,
                                    mocked_requests,
                                    pandas_series_IBM_volume):
        fc = TimeSeriesFinanceClient("IBM", api_key_str)

        ps = fc.weekly_volume()

        assert ps.count() == 1276

        assert ps.count() == pandas_series_IBM_volume.count()

        assert_series_equal(ps, pandas_series_IBM_volume)


    def test_weekly_volume_dates(self, api_key_str,
                                 mocked_requests,
                                 pandas_series_IBM_volume_filtered):
        fc = TimeSeriesFinanceClient("IBM", api_key_str)

        ps = fc.weekly_volume(dt.date(year=2021, month=1, day=1),
                              dt.date(year=2023, month=12, day=31))

        assert ps.count() == 156

        assert ps.count() == pandas_series_IBM_volume_filtered.count()

        assert_series_equal(ps, pandas_series_IBM_volume_filtered)

    def test_constructor_unsuccessful_request(self, api_key_str, mocked_requests):
        with pytest.raises(FinanceClientAPIError) as excinfo:
            TimeSeriesFinanceClient("NOCONNECT", api_key=api_key_str)
        assert "Unsuccessful API access" in str(excinfo.value)
