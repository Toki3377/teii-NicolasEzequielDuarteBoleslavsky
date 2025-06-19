import pytest
import requests
import teii.finance as tf

# Para simular un fallo de conexión, creamos una excepción que se comporte como una de 'requests'.
class MockConnectionError(requests.exceptions.ConnectionError):
    pass

def test_constructor_unsuccessful_request(monkeypatch):
    # Nuestra meta es que el constructor falle con un error de conexión.
    # Así que vamos a "engañar" a 'requests.get' para que lance un error en lugar de hacer una petición real.
    def mock_get(*args, **kwargs):
        raise MockConnectionError("¡Ups! Fallo de conexión simulado para la prueba.")

    # Con 'monkeypatch', sustituimos temporalmente la función 'get' de 'requests'.
    monkeypatch.setattr(requests, "get", mock_get)

    # La API key y el ticker no importan mucho aquí, ya que la conexión fallará de inmediato.
    api_key_str = "DA_IGUAL_CUAL_SEA"
    ticker = "TEST"

    # Esperamos que, al intentar crear el cliente, se lance una excepción específica: FinanceClientAPIError.
    with pytest.raises(tf.FinanceClientAPIError) as excinfo:
        tf.TimeSeriesFinanceClient(ticker, api_key_str)

    # Y para asegurarnos de que es el error correcto, verificamos que el mensaje contenga ciertas frases.
    assert "Error al contactar con la API de Alpha Vantage" in str(excinfo.value)
    assert "Fallo de conexión simulado para la prueba." in str(excinfo.value)
