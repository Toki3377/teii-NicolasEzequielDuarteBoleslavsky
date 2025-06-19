""" Time Series Finance Client classes """

import datetime as dt
import logging
from typing import Optional, Union

import pandas as pd

from teii.finance import FinanceClient, FinanceClientInvalidData, FinanceClientParamError # Importa FinanceClientParamError


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
                 logging_level: Union[int, str] = logging.WARNING) -> None:
        """ TimeSeriesFinanceClient constructor. """

        super().__init__(ticker, api_key, logging_level)

        self._build_data_frame()

    def _build_data_frame(self) -> None:
        """ Build Panda's DataFrame and format data. """

        # TODO
        #   Comprueba que no se produce ningún error y genera excepción
        #   'FinanceClientInvalidData' en caso contrario
        if not self._json_data: # Verifica si _json_data está vacío o es None
            raise FinanceClientInvalidData("No weekly adjusted time series data found in API response.")

        try:
            # Build Panda's data frame
            data_frame = pd.DataFrame.from_dict(self._json_data, orient='index', dtype='float')

            # Comprobación adicional: si el DataFrame está vacío después de la creación
            if data_frame.empty:
                raise FinanceClientInvalidData("DataFrame is empty after parsing JSON data.")

            # Rename data fields
            data_frame = data_frame.rename(columns={key: name_type[0]
                                                    for key, name_type in self._data_field2name_type.items()})

            # Set data field types
            data_frame = data_frame.astype(dtype={name_type[0]: name_type[1]
                                                  for key, name_type in self._data_field2name_type.items()})

            # Set index type
            data_frame.index = data_frame.index.astype("datetime64[ns]")

            # Sort data
            self._data_frame = data_frame.sort_index(ascending=True)

        except Exception as e:
            # Captura cualquier otro error durante el procesamiento del DataFrame
            raise FinanceClientInvalidData(f"Error building DataFrame from API response: {e}") from e

    def _build_base_query_url_params(self) -> str:
        """ Return base query URL parameters.

        Parameters are dependent on the query type:
            https://www.alphavantage.co/documentation/
        URL format:
            https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol=TICKER&outputsize=full&apikey=API_KEY&data_type=json
        """

        return f"function=TIME_SERIES_WEEKLY_ADJUSTED&symbol={self._ticker}&outputsize=full&apikey={self._api_key}"

    @classmethod
    def _build_query_data_key(cls) -> str:
        """ Return data query key. """

        return "Weekly Adjusted Time Series"

    def _validate_query_data(self) -> None:
        """ Validate query data. """

        try:
            assert self._json_metadata["2. Symbol"] == self._ticker
        except Exception as e:
            raise FinanceClientInvalidData("Metadata field '2. Symbol' not found") from e
        else:
            self._logger.info(f"Metadata key '2. Symbol' = '{self._ticker}' found")

    def weekly_price(self,
                     from_date: Optional[dt.date] = None,
                     to_date: Optional[dt.date] = None) -> pd.Series:
        """ Return weekly close price from 'from_date' to 'to_date'. """

        assert self._data_frame is not None

        series = self._data_frame['aclose']

        if from_date is not None and to_date is not None:
            # Comprobación de que from_date no sea mayor que to_date
            if from_date > to_date:
                raise FinanceClientParamError("'from_date' cannot be greater than 'to_date'")
            series = series.loc[from_date:to_date]   # type: ignore

        return series

    def weekly_volume(self,
                      from_date: Optional[dt.date] = None,
                      to_date: Optional[dt.date] = None) -> pd.Series:
        """ Return weekly volume from 'from_date' to 'to_date'. """

        assert self._data_frame is not None

        series = self._data_frame['volume']

        # TODO
        #   Comprueba que from_date <= to_date y genera excepción
        #   'FinanceClientParamError' en caso de error
        if from_date is not None and to_date is not None:
            if from_date > to_date:
                raise FinanceClientParamError("'from_date' cannot be greater than 'to_date'")
            series = series.loc[from_date:to_date]   # type: ignore

        return series
        
   def yearly_dividends(self,
                         from_year: Optional[int] = None,
                         to_year: Optional[int] = None) -> pd.Series:
        """
        Calculate total annual dividends from 'from_year' to 'to_year'.

        Parameters:
            from_year (Optional[int]): The starting year for filtering dividends.
            to_year (Optional[int]): The ending year for filtering dividends.

        Returns:
            pd.Series: A Series with years as index and total annual dividends as values.

        Raises:
            FinanceClientParamError: If from_year is greater than to_year.
        """
        assert self._data_frame is not None, "DataFrame has not been built."

        dividends_series = self._data_frame['dividend']

        # Filter out zero dividends to avoid unnecessary processing
        dividends_series = dividends_series[dividends_series > 0]

        if from_year is not None and to_year is not None:
            if from_year > to_year:
                raise FinanceClientParamError("'from_year' cannot be greater than 'to_year'")

            # Filter by year range
            dividends_series = dividends_series[
                (dividends_series.index.year >= from_year) &
                (dividends_series.index.year <= to_year)
            ]
        elif from_year is not None:
            dividends_series = dividends_series[dividends_series.index.year >= from_year]
        elif to_year is not None:
            dividends_series = dividends_series[dividends_series.index.year <= to_year]

        # Group by year and sum dividends
        yearly_total_dividends = dividends_series.groupby(dividends_series.index.year).sum()

        # Rename index to 'year' for clarity if needed, or leave as int for Series
        yearly_total_dividends.index.name = 'year'

        return yearly_total_dividends
        
       def highest_weekly_variation(self,
                                 from_date: Optional[dt.date] = None,
                                 to_date: Optional[dt.date] = None) -> Tuple[dt.date, float, float, float]:
        """
        Calculates the date with the highest weekly price variation (high - low).

        Parameters:
            from_date (Optional[dt.date]): The starting date for filtering.
            to_date (Optional[dt.date]): The ending date for filtering.

        Returns:
            Tuple[dt.date, float, float, float]: A tuple containing the date,
                                                  high price, low price, and the high-low variation.

        Raises:
            FinanceClientParamError: If from_date is greater than to_date.
            FinanceClientInvalidData: If no data is available for the specified range.
        """
        assert self._data_frame is not None, "DataFrame has not been built."

        df_variation = self._data_frame[['high', 'low']].copy()
        
        if from_date is not None and to_date is not None:
            if from_date > to_date:
                raise FinanceClientParamError("'from_date' cannot be greater than 'to_date'")
            df_variation = df_variation.loc[from_date:to_date]
        elif from_date is not None:
            df_variation = df_variation.loc[from_date:]
        elif to_date is not None:
            df_variation = df_variation.loc[:to_date]

        if df_variation.empty:
            raise FinanceClientInvalidData("No data available for the specified date range.")

        df_variation['variation'] = df_variation['high'] - df_variation['low']

        max_variation_date = df_variation['variation'].idxmax()
        max_variation_row = df_variation.loc[max_variation_date]

        high_val = max_variation_row['high']
        low_val = max_variation_row['low']
        variation_val = max_variation_row['variation']

        # Ensure the date is a datetime.date object
        date_obj = max_variation_date.to_pydatetime().date() if isinstance(max_variation_date, pd.Timestamp) else max_variation_date.date()

        return (date_obj, high_val, low_val, variation_val)
