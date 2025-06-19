""" Unit tests for teii.finance.timeseries module """


import datetime as dt

import pytest
import requests
from pandas.testing import assert_series_equal

from teii.finance import FinanceClientInvalidAPIKey, FinanceClientParamError, FinanceClientAPIError, FinanceClientInvalidData, TimeSeriesFinanceClient


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

IBM_VALID_DATA = {
    "Meta Data": {
        "1. Information": "Weekly Adjusted Prices and Volumes",
        "2. Symbol": "IBM",
        "3. Last Refreshed": "2025-06-19",
        "4. Time Zone": "US/Eastern"
    },
    "Weekly Adjusted Time Series": {
        "2025-01-31": {
            "1. open": "230.00", "2. high": "261.80", "3. low": "219.84",
            "4. close": "240.00", "5. adjusted close": "240.00",
            "6. volume": "10000000", "7. dividend amount": "0.00"
        },
        "2025-06-13": {
            "1. open": "180.00", "2. high": "182.00", "3. low": "178.00",
            "4. close": "181.00", "5. adjusted close": "181.00",
            "6. volume": "400000", "7. dividend amount": "1.00"
        },
        "2025-03-14": {
            "1. open": "175.00", "2. high": "178.00", "3. low": "174.00",
            "4. close": "176.00", "5. adjusted close": "176.00",
            "6. volume": "450000", "7. dividend amount": "0.50"
        },
        "2024-12-20": {
            "1. open": "170.00", "2. high": "173.00", "3. low": "169.00",
            "4. close": "172.00", "5. adjusted close": "172.00",
            "6. volume": "500000", "7. dividend amount": "1.65"
        },
        "2024-09-20": {
            "1. open": "165.00", "2. high": "168.00", "3. low": "164.00",
            "4. close": "167.00", "5. adjusted close": "167.00",
            "6. volume": "550000", "7. dividend amount": "1.65"
        },
        "2024-06-21": {
            "1. open": "160.00", "2. high": "163.00", "3. low": "159.00",
            "4. close": "162.00", "5. adjusted close": "162.00",
            "6. volume": "600000", "7. dividend amount": "1.65"
        },
        "2024-03-22": {
            "1. open": "155.00", "2. high": "158.00", "3. low": "154.00",
            "4. close": "157.00", "5. adjusted close": "157.00",
            "6. volume": "650000", "7. dividend amount": "1.65"
        },
        "2023-10-27": {
            "1. open": "145.00", "2. high": "148.00", "3. low": "144.00",
            "4. close": "147.00", "5. adjusted close": "147.00",
            "6. volume": "500000", "7. dividend amount": "1.65"
        },
        "2023-10-20": {
            "1. open": "140.00", "2. high": "145.00", "3. low": "139.00",
            "4. close": "142.00", "5. adjusted close": "142.00",
            "6. volume": "600000", "7. dividend amount": "1.65"
        },
        "2023-10-13": {
            "1. open": "138.00", "2. high": "142.00", "3. low": "137.00",
            "4. close": "140.00", "5. adjusted close": "140.00",
            "6. volume": "700000", "7. dividend amount": "1.65"
        },
        "2023-10-06": {
            "1. open": "135.00", "2. high": "139.00", "3. low": "134.00",
            "4. close": "137.00", "5. adjusted close": "137.00",
            "6. volume": "800000", "7. dividend amount": "1.65"
        },
        "2000-10-20": {
            "1. open": "95.00", "2. high": "113.87", "3. low": "90.25",
            "4. close": "100.00", "5. adjusted close": "100.00",
            "6. volume": "8000000", "7. dividend amount": "0.00"
        },
        "2020-03-13": {
            "1. open": "110.00", "2. high": "124.88", "3. low": "100.81",
            "4. close": "115.00", "5. adjusted close": "115.00",
            "6. volume": "9000000", "7. dividend amount": "0.00"
        },
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
    requests_mock.get(
        "https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol=IBM&outputsize=full&apikey=TEST",
        json=IBM_VALID_DATA
    )
    requests_mock.get(
        "https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol=NOCONNECT&outputsize=full&apikey=TEST",
        exc=requests.exceptions.ConnectionError
    )
    return requests_mock


# Asumiendo que TEST_DATA_PATH y los fixtures de pandas_series_IBM_... están definidos en otro lugar
# como conftest.py o directamente en este archivo.
from pathlib import Path
TEST_DATA_PATH = Path(__file__).parent / "data"


class TestTimeSeriesFinanceClient:

    def test_constructor_success(self, api_key_str, mocked_requests):
        TimeSeriesFinanceClient("IBM", api_key_str)


    def test_constructor_failure_invalid_api_key(self):
        with pytest.raises(FinanceClientInvalidAPIKey):
            TimeSeriesFinanceClient("IBM")

    def test_constructor_invalid_data(self, mocked_requests):
        with pytest.raises(FinanceClientInvalidData) as excinfo:
            TimeSeriesFinanceClient("NODATA", api_key="TEST", logging_level=logging.CRITICAL)
        assert "No weekly adjusted time series data found in API response." in str(excinfo.value)


    def test_weekly_price_invalid_dates(self, api_key_str, mocked_requests):
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

    def test_yearly_dividends_unfiltered(self, api_key_str, mocked_requests):
        fc = TimeSeriesFinanceClient("IBM", api_key_str)
        expected_series = pd.read_csv(
            TEST_DATA_PATH / "TIME_SERIES_WEEKLY_ADJUSTED.IBM.yearly_dividend.unfiltered.csv",
            index_col='year',
            dtype={'dividend': float}
        )['dividend']
        expected_series.index = expected_series.index.astype(int)
        actual_series = fc.yearly_dividends()
        pd.testing.assert_series_equal(actual_series, expected_series)

    def test_yearly_dividends_filtered(self, api_key_str, mocked_requests):
        fc = TimeSeriesFinanceClient("IBM", api_key_str)
        from_year = 2023
        to_year = 2024
        expected_series = pd.read_csv(
            TEST_DATA_PATH / "TIME_SERIES_WEEKLY_ADJUSTED.IBM.yearly_dividend.filtered.csv",
            index_col='year',
            dtype={'dividend': float}
        )['dividend']
        expected_series.index = expected_series.index.astype(int)
        actual_series = fc.yearly_dividends(from_year=from_year, to_year=to_year)
        pd.testing.assert_series_equal(actual_series, expected_series)

    def test_yearly_dividends_invalid_years(self, api_key_str, mocked_requests):
        fc = TimeSeriesFinanceClient("IBM", api_key_str)
        future_year = 2026
        past_year = 2025
        with pytest.raises(FinanceClientParamError) as excinfo:
            fc.yearly_dividends(from_year=future_year, to_year=past_year)
        assert "'from_year' cannot be greater than 'to_year'" in str(excinfo.value)

    def test_highest_weekly_variation_unfiltered(self, api_key_str, mocked_requests):
        fc = TimeSeriesFinanceClient("IBM", api_key_str)
        result = fc.highest_weekly_variation()
        expected_date = dt.date.fromisoformat('2025-01-31')
        expected_high = 261.8
        expected_low = 219.84
        expected_variation = 41.96
        assert result == (expected_date, expected_high, expected_low, expected_variation)

    def test_highest_weekly_variation_filtered_2000_2010(self, api_key_str, mocked_requests):
        fc = TimeSeriesFinanceClient("IBM", api_key_str)
        result = fc.highest_weekly_variation(from_date=dt.date(2000, 1, 1), to_date=dt.date(2010, 12, 31))
        expected_date = dt.date.fromisoformat('2000-10-20')
        expected_high = 113.87
        expected_low = 90.25
        expected_variation = 23.62
        assert result == (expected_date, expected_high, expected_low, expected_variation)

    def test_highest_weekly_variation_filtered_2011_2023(self, api_key_str, mocked_requests):
        fc = TimeSeriesFinanceClient("IBM", api_key_str)
        result = fc.highest_weekly_variation(from_date=dt.date(2011, 1, 1), to_date=dt.date(2023, 12, 31))
        expected_date = dt.date.fromisoformat('2020-03-13')
        expected_high = 124.88
        expected_low = 100.81
        expected_variation = 24.07
        assert result == (expected_date, expected_high, expected_low, expected_variation)

    def test_highest_weekly_variation_invalid_dates(self, api_key_str, mocked_requests):
        fc = TimeSeriesFinanceClient("IBM", api_key_str)
        with pytest.raises(FinanceClientParamError) as excinfo:
            fc.highest_weekly_variation(from_date=dt.date(2025, 1, 1), to_date=dt.date(2024, 12, 31))
        assert "'from_date' cannot be greater than 'to_date'" in str(excinfo.value)

    def test_highest_weekly_variation_no_data_in_range(self, api_key_str, mocked_requests):
        fc = TimeSeriesFinanceClient("IBM", api_key_str)
        with pytest.raises(FinanceClientInvalidData) as excinfo:
            fc.highest_weekly_variation(from_date=dt.date(1990, 1, 1), to_date=dt.date(1990, 12, 31))
        assert "No data available for the specified date range." in str(excinfo.value)
