""" Time Series Finance Client classes """

import datetime as dt
import logging
import pandas as pd
from typing import Optional, Union, Tuple

from teii.finance import FinanceClient, FinanceClientInvalidData, FinanceClientParamError


class TimeSeriesFinanceClient(FinanceClient):
    """ Wrapper around the AlphaVantage API for Time Series Weekly Adjusted.

        Source:
            https://www.alphavantage.co/documentation/ (TIME_SERIES_WEEKLY_ADJUSTED)
    """

    _data_field2name_type = {
        "1. open":                  ("open",     "float"),
        "2. high":                  ("high",     "float"),
        "3. low":                   ("low",      "float"),
        "4. close":                 ("close",    "float"),
        "5. adjusted close":        ("aclose",   "float"),
        "6. volume":                ("volume",   "int"),
        "7. dividend amount":       ("dividend", "float")
    }

    def __init__(self, ticker: str,
                 api_key: Optional[str] = None,
                 logging_level: Union[int, str] = logging.WARNING,
                 logging_file: Optional[str] = None) -> None:
        """ TimeSeriesFinanceClient constructor. """

        # Inicialización del cliente con el ticker proporcionado
        super().__init__(ticker, api_key, logging_level, logging_file)
        self._build_data_frame()
        self._logger.info(f"Client for {ticker} initialized.")

    def _build_data_frame(self) -> None:
        """ Build Panda's DataFrame and format data. """
        # Verifica que los datos JSON no estén vacíos antes de construir el DataFrame
        if not self._json_data:
            self._logger.error("No data found in JSON response.")
            raise FinanceClientInvalidData("No weekly adjusted time series data found in API response.")

        try:
            # Construye el DataFrame a partir del diccionario JSON
            data_frame = pd.DataFrame.from_dict(self._json_data, orient='index', dtype='float')

            # Valida si el DataFrame resultante está vacío
            if data_frame.empty:
                self._logger.error("DataFrame is empty after parsing JSON.")
                raise FinanceClientInvalidData("DataFrame is empty after parsing JSON data.")

            # Renombra las columnas para usar nombres más amigables
            data_frame = data_frame.rename(columns={key: name_type[0]
                                                    for key, name_type in self._data_field2name_type.items()})

            # Establece los tipos de datos correctos para cada columna
            data_frame = data_frame.astype(dtype={name_type[0]: name_type[1]
                                                  for key, name_type in self._data_field2name_type.items()})

            # Convierte el índice a tipo datetime para facilitar las operaciones de fecha
            data_frame.index = data_frame.index.astype("datetime64[ns]")

            # Ordena los datos por fecha en orden ascendente
            self._data_frame = data_frame.sort_index(ascending=True)
            self._logger.info("DataFrame built and sorted.")

        except Exception as e:
            # Captura y relanza cualquier error durante la construcción del DataFrame
            self._logger.critical(f"Error building DataFrame: {e}")
            raise FinanceClientInvalidData(f"Error building DataFrame from API response: {e}") from e

    def _build_base_query_url_params(self) -> str:
        """ Return base query URL parameters.

        Parameters are dependent on the query type:
            https://www.alphavantage.co/documentation/
        URL format:
            https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol=TICKER&outputsize=full&apikey=API_KEY&data_type=json
        """
        # Construye la cadena de parámetros de la URL para la API
        return f"function=TIME_SERIES_WEEKLY_ADJUSTED&symbol={self._ticker}&outputsize=full&apikey={self._api_key}"

    @classmethod
    def _build_query_data_key(cls) -> str:
        """ Return data query key. """
        # Define la clave principal para acceder a los datos de la serie temporal en la respuesta JSON
        cls_logger = logging.getLogger(cls.__name__)
        cls_logger.debug("Retrieving data query key.")
        return "Weekly Adjusted Time Series"

    def _validate_query_data(self) -> None:
        """ Validate query data. """
        # Valida que el símbolo del ticker en los metadatos de la respuesta coincida con el solicitado
        try:
            assert self._json_metadata["2. Symbol"] == self._ticker
        except Exception as e:
            self._logger.error(f"Metadata symbol '{self._json_metadata.get('2. Symbol', 'N/A')}' mismatch for ticker '{self._ticker}'.")
            raise FinanceClientInvalidData("Metadata field '2. Symbol' not found") from e
        else:
            self._logger.info(f"Metadata symbol '{self._ticker}' validated.")

    def weekly_price(self,
                     from_date: Optional[dt.date] = None,
                     to_date: Optional[dt.date] = None) -> pd.Series:
        """ Return weekly close price from 'from_date' to 'to_date'. """
        assert self._data_frame is not None

        series = self._data_frame['aclose']

        # Aplica filtrado por rango de fechas si se proporcionan los parámetros
        if from_date is not None and to_date is not None:
            if from_date > to_date:
                self._logger.error(f"Invalid date range: {from_date} after {to_date}.")
                raise FinanceClientParamError("'from_date' cannot be greater than 'to_date'")
            series = series.loc[from_date:to_date]   # type: ignore
            self._logger.info(f"Weekly price filtered from {from_date} to {to_date}.")
        elif from_date is not None:
            series = series.loc[from_date:]
            self._logger.info(f"Weekly price filtered from {from_date}.")
        elif to_date is not None:
            series = series.loc[:to_date]
            self._logger.info(f"Weekly price filtered up to {to_date}.")
        else:
            self._logger.info("Returning unfiltered weekly price.")

        return series

    def weekly_volume(self,
                      from_date: Optional[dt.date] = None,
                      to_date: Optional[dt.date] = None) -> pd.Series:
        """ Return weekly volume from 'from_date' to 'to_date'. """
        assert self._data_frame is not None

        series = self._data_frame['volume']

        # Aplica filtrado por rango de fechas si se proporcionan los parámetros
        if from_date is not None and to_date is not None:
            if from_date > to_date:
                self._logger.error(f"Invalid date range: {from_date} after {to_date}.")
                raise FinanceClientParamError("'from_date' cannot be greater than 'to_date'")
            series = series.loc[from_date:to_date]   # type: ignore
            self._logger.info(f"Weekly volume filtered from {from_date} to {to_date}.")
        elif from_date is not None:
            series = series.loc[from_date:]
            self._logger.info(f"Weekly volume filtered from {from_date}.")
        elif to_date is not None:
            series = series.loc[:to_date]
            self._logger.info(f"Weekly volume filtered up to {to_date}.")
        else:
            self._logger.info("Returning unfiltered weekly volume.")

        return series

    def yearly_dividends(self,
                         from_year: Optional[int] = None,
                         to_year: Optional[int] = None) -> pd.Series:
        """
        Calculate total annual dividends from 'from_year' to 'to_year'.
        """
        assert self._data_frame is not None, "DataFrame has not been built."

        dividends_series = self._data_frame['dividend']

        # Filtra los dividendos que son cero
        dividends_series = dividends_series[dividends_series > 0]
        self._logger.debug(f"Filtered {len(dividends_series)} non-zero dividends.")

        # Aplica filtrado por rango de años si se proporcionan los parámetros
        if from_year is not None and to_year is not None:
            if from_year > to_year:
                self._logger.error(f"Invalid year range: {from_year} after {to_year}.")
                raise FinanceClientParamError("'from_year' cannot be greater than 'to_year'")

            dividends_series = dividends_series[
                (dividends_series.index.year >= from_year) &
                (dividends_series.index.year <= to_year)
            ]
            self._logger.info(f"Dividends filtered from year {from_year} to {to_year}.")
        elif from_year is not None:
            dividends_series = dividends_series[dividends_series.index.year >= from_year]
            self._logger.info(f"Dividends filtered from year {from_year}.")
        elif to_year is not None:
            dividends_series = dividends_series[dividends_series.index.year <= to_year]
            self._logger.info(f"Dividends filtered up to year {to_year}.")
        else:
            self._logger.info("Calculating unfiltered yearly dividends.")


        # Agrupa los dividendos por año y suma los valores
        yearly_total_dividends = dividends_series.groupby(dividends_series.index.year).sum()
        self._logger.info("Yearly total dividends calculated.")

        # Establece el nombre del índice como 'year'
        yearly_total_dividends.index.name = 'year'

        return yearly_total_dividends

    def highest_weekly_variation(self,
                                 from_date: Optional[dt.date] = None,
                                 to_date: Optional[dt.date] = None) -> Tuple[dt.date, float, float, float]:
        """
        Calculates the date with the highest weekly price variation (high - low).
        """
        assert self._data_frame is not None, "DataFrame has not been built."

        # Crea un DataFrame temporal con solo las columnas 'high' y 'low'
        df_variation = self._data_frame[['high', 'low']].copy()
        
        # Aplica filtrado por rango de fechas si se proporcionan los parámetros
        if from_date is not None and to_date is not None:
            if from_date > to_date:
                self._logger.error(f"Invalid date range: {from_date} after {to_date}.")
                raise FinanceClientParamError("'from_date' cannot be greater than 'to_date'")
            df_variation = df_variation.loc[from_date:to_date]
            self._logger.debug(f"Variation data filtered from {from_date} to {to_date}.")
        elif from_date is not None:
            df_variation = df_variation.loc[from_date:]
            self._logger.debug(f"Variation data filtered from {from_date}.")
        elif to_date is not None:
            df_variation = df_variation.loc[:to_date]
            self._logger.debug(f"Variation data filtered up to {to_date}.")
        else:
            self._logger.debug("Analyzing unfiltered variation data.")


        # Verifica si hay datos disponibles después del filtrado
        if df_variation.empty:
            self._logger.warning("No data for variation calculation in the specified range.")
            raise FinanceClientInvalidData("No data available for the specified date range.")

        # Calcula la variación semanal (high - low)
        df_variation['variation'] = df_variation['high'] - df_variation['low']

        # Encuentra la fecha con la mayor variación
        max_variation_date = df_variation['variation'].idxmax()
        max_variation_row = df_variation.loc[max_variation_date]
        self._logger.info(f"Highest variation found on {max_variation_date}.")

        # Extrae los valores de high, low y la variación
        high_val = max_variation_row['high']
        low_val = max_variation_row['low']
        variation_val = max_variation_row['variation']

        # Convierte el índice de fecha a un objeto datetime.date
        date_obj = max_variation_date.to_pydatetime().date() if isinstance(max_variation_date, pd.Timestamp) else max_variation_date.date()
        self._logger.debug(f"Returning highest variation details: {date_obj}, {high_val}, {low_val}, {variation_val}.")

        return (date_obj, high_val, low_val, variation_val)
