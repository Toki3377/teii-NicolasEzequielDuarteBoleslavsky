import datetime as dt
import logging
import pytest
import pandas as pd
from pathlib import Path

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

IBM_VALID_DATA = {
    "Meta Data": {
        "1. Information": "Weekly Adjusted Prices and Volumes",
        "2. Symbol": "IBM",
        "3. Last Refreshed": "2025-06-19",
        "4. Time Zone": "US/Eastern"
    },
    "Weekly Adjusted Time Series": {
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
    requests_mock.get(
        "https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol=IBM&outputsize=full&apikey=TEST",
        json=IBM_VALID_DATA
    )
    return requests_mock


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

    def test_yearly_dividends_unfiltered(self, mocked_requests):
        """
        Test yearly_dividends() method returns the correct unfiltered total annual dividends series
        matching the reference CSV.
        """
        client = TimeSeriesFinanceClient("IBM", api_key="TEST", logging_level=logging.CRITICAL)
        
        expected_series = pd.read_csv(
            TEST_DATA_PATH / "TIME_SERIES_WEEKLY_ADJUSTED.IBM.yearly_dividend.unfiltered.csv",
            index_col='year',
            parse_dates=True,
            dtype={'dividend': float}
        )['dividend']
        expected_series.index = expected_series.index.astype(int)

        actual_series = client.yearly_dividends()

        pd.testing.assert_series_equal(actual_series, expected_series)

    def test_yearly_dividends_filtered(self, mocked_requests):
        """
        Test yearly_dividends() method returns the correct filtered total annual dividends series
        matching the reference CSV for a specific year range.
        """
        client = TimeSeriesFinanceClient("IBM", api_key="TEST", logging_level=logging.CRITICAL)

        from_year = 2023
        to_year = 2024
        
        expected_series = pd.read_csv(
            TEST_DATA_PATH / "TIME_SERIES_WEEKLY_ADJUSTED.IBM.yearly_dividend.filtered.csv",
            index_col='year',
            parse_dates=True,
            dtype={'dividend': float}
        )['dividend']
        expected_series.index = expected_series.index.astype(int)

        actual_series = client.yearly_dividends(from_year=from_year, to_year=to_year)

        pd.testing.assert_series_equal(actual_series, expected_series)

    def test_yearly_dividends_invalid_years(self, mocked_requests):
        """
        Test that FinanceClientParamError is raised when from_year is greater than to_year
        in yearly_dividends method.
        """
        client = TimeSeriesFinanceClient("IBM", api_key="TEST", logging_level=logging.CRITICAL)

        future_year = 2026
        past_year = 2025

        with pytest.raises(FinanceClientParamError) as excinfo:
            client.yearly_dividends(from_year=future_year, to_year=past_year)

        assert "'from_year' cannot be greater than 'to_year'" in str(excinfo.value)
