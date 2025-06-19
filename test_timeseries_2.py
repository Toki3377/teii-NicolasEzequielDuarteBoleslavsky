import datetime as dt
import logging
import pytest
import json

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
    return requests_mock


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
