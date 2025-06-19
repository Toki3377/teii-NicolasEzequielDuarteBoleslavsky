""" Unit tests for teii.finance.timeseries module """


import datetime as dt

import pytest
from pandas.testing import assert_series_equal

from teii.finance import FinanceClientInvalidAPIKey, FinanceClientParamError, TimeSeriesFinanceClient


def test_constructor_success(api_key_str,
                             mocked_requests):
    TimeSeriesFinanceClient("IBM", api_key_str)


def test_constructor_failure_invalid_api_key():
    with pytest.raises(FinanceClientInvalidAPIKey):
        TimeSeriesFinanceClient("IBM")


def test_weekly_price_invalid_dates(api_key_str,
                                    mocked_requests):
    fc = TimeSeriesFinanceClient("IBM", api_key_str)
    with pytest.raises(FinanceClientParamError):
        fc.weekly_price(dt.date(year=2024, month=1, day=1),
                        dt.date(year=2023, month=1, day=1))


def test_weekly_price_no_dates(api_key_str,
                               mocked_requests,
                               pandas_series_IBM_prices):
    fc = TimeSeriesFinanceClient("IBM", api_key_str)

    ps = fc.weekly_price()

    assert ps.count() == 1276

    assert ps.count() == pandas_series_IBM_prices.count()

    assert_series_equal(ps, pandas_series_IBM_prices)


def test_weekly_price_dates(api_key_str,
                            mocked_requests,
                            pandas_series_IBM_prices_filtered):
    fc = TimeSeriesFinanceClient("IBM", api_key_str)

    ps = fc.weekly_price(dt.date(year=2021, month=1, day=1),
                         dt.date(year=2023, month=12, day=31))

    assert ps.count() == 156

    assert ps.count() == pandas_series_IBM_prices_filtered.count()

    assert_series_equal(ps, pandas_series_IBM_prices_filtered)


def test_weekly_volume_invalid_dates(api_key_str,
                                     mocked_requests):
    fc = TimeSeriesFinanceClient("IBM", api_key_str)
    with pytest.raises(FinanceClientParamError):
        fc.weekly_volume(dt.date(year=2024, month=1, day=1),
                         dt.date(year=2023, month=1, day=1))


def test_weekly_volume_no_dates(api_key_str,
                                mocked_requests,
                                pandas_series_IBM_volume):
    fc = TimeSeriesFinanceClient("IBM", api_key_str)

    ps = fc.weekly_volume()

    assert ps.count() == 1276

    assert ps.count() == pandas_series_IBM_volume.count()

    assert_series_equal(ps, pandas_series_IBM_volume)


def test_weekly_volume_dates(api_key_str,
                             mocked_requests,
                             pandas_series_IBM_volume_filtered):
    fc = TimeSeriesFinanceClient("IBM", api_key_str)

    ps = fc.weekly_volume(dt.date(year=2021, month=1, day=1),
                          dt.date(year=2023, month=12, day=31))

    assert ps.count() == 156

    assert ps.count() == pandas_series_IBM_volume_filtered.count()

    assert_series_equal(ps, pandas_series_IBM_volume_filtered)
