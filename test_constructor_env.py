import pytest
import os
import teii.finance as tf

def test_constructor_env(monkeypatch):
    monkeypatch.setenv("TEII_FINANCE_API_KEY", "FAKE_API_KEY_FROM_ENV")
    try:
        client = tf.TimeSeriesFinanceClient(ticker='TEST', api_key_str=None)
        assert client is not None
        assert client._api_key == "FAKE_API_KEY_FROM_ENV"
    except Exception as e:
        pytest.fail(f"El constructor falló inesperadamente: {e}")
    monkeypatch.delenv("TEII_FINANCE_API_KEY", raising=False)
