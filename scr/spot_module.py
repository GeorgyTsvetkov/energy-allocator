"""
TODO program def update_cache function to take into account months, days and 
hours, not just years
"""

import requests
from datetime import datetime
import pandas as pd
import traceback
from auxiliary_module import AuxiliaryVar, AuxiliaryFunc
import os
import logging
logging.basicConfig(level=logging.DEBUG)

class SpotMedianCalculator:
    """
    A utility class to fetch, cache, and analyze hourly Finnish spot electricity 
    prices from the Sahkotin API. It allows for aggregation and comparison 
    of hourly price medians across years, including optional VAT and transfer 
    fees.
    """


# CLASS CONSTANTS ------------------------------------------------------------
    def __init__(
                self,
                analysis_length         :int                = 5,
                vat_perc                :float              = 0.255,
                transfer_fee_perc       :float              = 0.111,
                spot_cache_rel_dir      :str                = 'assets',
                spot_cache_file         :str                = 'spot_price_data.csv',
                ):
        f"""
        Initializes the SpotMedianCalculator with analysis configuration 
        and file paths for cached spot price data.

        Args:
            analysis_length (int): Number of years to include in analysis (max 13).
            vat_perc (float): VAT percentage as a decimal (e.g., 0.24).
            transfer_fee_perc (float): Transfer fee percentage as a decimal.
            spot_cache_rel_dir (str): Relative directory for the cached CSV file.
            spot_cache_file (str): Filename for the cached spot price data.
        """

        
        # Analysis length limit
        if analysis_length > 13:
            analysis_length = 13

        self.analysis_length        = analysis_length
        self.additional_fee         = (1 + vat_perc+transfer_fee_perc)
        self.spot_cache_rel_dir     = spot_cache_rel_dir
        self.spot_cache_file        = spot_cache_file
        self.auxiliary_var          = AuxiliaryVar()
        self.auxiliary_func         = AuxiliaryFunc()

        self.spot_price_median              = 'Spot median [c/kWh]'
        self.spot_price_median_fees         = 'Spot median w/ fees [c/kWh]'

# SPOT PRICE PROCESSING FUNCTIONS ------------------------------------
    def spot_get_price(self) -> pd.DataFrame:
        """
        Retrieves hourly Finnish spot electricity prices as a DataFrame.

        Loads data from a cached CSV if available. If the cached data is missing or 
        incomplete for the requested period, it updates the cache using the Sahkotin API.

        The resulting DataFrame is limited to the configured number of most recent 
        full years and indexed by hourly UTC timestamps.

        Returns:
            pd.DataFrame: Hourly DataFrame with one column:
                - 'value' (float): Spot price in c/kWh (snt/kWh).
        """


        def cached_csv_to_dataframe(filepath:str) -> pd.DataFrame:
            """
            Loads cached spot price data from a CSV file into a DataFrame.

            Args:
                filepath (str): Path to the CSV file.

            Returns:
                pd.DataFrame: Time-indexed DataFrame containing:
                    - 'value' (float): Spot price in c/kWh.
            """

            # Read CSV file as a Pandas DataFrame
            dataframe           = pd.read_csv(filepath)
            dataframe.columns = dataframe.columns.str.strip().str.lower()
            # Set either 'date' or first column as datetime index
            if 'date' in dataframe.columns:
                dataframe['date']   = pd.to_datetime(dataframe['date'], utc=True)
                dataframe.set_index('date', inplace=True)
            else:
                dataframe[0]   = pd.to_datetime(dataframe[0], utc=True)
                dataframe.set_index(0, inplace=True)
            logging.info("Spot data CSV successfully converted to dataframe")
            return dataframe

        def from_api_to_dataframe(start:str, end:str) -> pd.DataFrame:
            """
            Fetches Finnish spot price data from the Sahkotin API and 
            returns it as a DataFrame.

            Args:
                start (str): Start datetime in ISO 8601 format (%Y-%m-%dT%H:%M:%S.000Z).
                end (str): End datetime in ISO 8601 format (%Y-%m-%dT%H:%M:%S.000Z).

            Returns:
                pd.DataFrame: Hourly time-indexed DataFrame with:
                    - 'value' (float): Spot price in c/kWh.
            """

            # Establish connection with API, raise for status if needed
            url             = f"https://sahkotin.fi/prices?start={start}&end={end}"
            response        = requests.get(url)
            response.raise_for_status()
            # Parse API response as JSON and extract values, price = snt/kWh
            spot            = response.json()
            date_price_list = [
                            {'date':entry['date'], 'value': entry['value'] * 0.1}
                            for entries in spot.values()
                            for entry in entries
            ]
            # Form datetime indexed Pandas DataFrame from parsed API response
            dataframe   = pd.DataFrame(date_price_list)
            dataframe['date']        = pd.to_datetime(dataframe['date'], utc=True)
            dataframe.set_index('date', inplace=True)
            # Check if data in 15 minute resolution, convert to hourly
            dataframe   = self.auxiliary_func.resample_quarter_to_hour(dataframe)
            logging.info('Spot data got from API and converter to dataframe')
            return dataframe
        
        def update_cache(dataframe:pd.DataFrame) -> pd.DataFrame:
            """
            Updates the cached spot price data by checking whether the stored 
            date range covers the required analysis years.

            If the cache is missing data from earlier or later years, it fetches 
            and appends/prepends the missing data via API.

            TODO: Improve granularity of cache update to include months, days, and hours.

            Args:
                dataframe (pd.DataFrame): Existing DataFrame loaded from cache.

            Returns:
                pd.DataFrame: Updated DataFrame with all required years included.
            """

            if dataframe.index.year.max() < end_year:
                start       = f"{dataframe.index.year.max()+1}-01-01T00:00:00.000Z"
                end         = f"{end_year}-12-31T23:00:00Z"
                dataframe   = from_api_to_dataframe(start, end)
                dataframe.to_csv(filepath, mode='a', header=not os.path.exists(filepath))
                dataframe:pd.DataFrame = cached_csv_to_dataframe(filepath)
                logging.info('Spot data CSV updated from API')
                return dataframe
            elif dataframe.index.year.min() > start_year:
                start       = f"{start_year}-01-01T00:00:00.000Z"
                end         = f"{dataframe.index.year.min()-1}-12-31T23:00:00Z"
                new_df      = from_api_to_dataframe(start, end)
                combined_df = pd.concat([new_df, dataframe], ignore_index=False)
                combined_df.to_csv(filepath)
                dataframe:pd.DataFrame = cached_csv_to_dataframe(filepath)
                logging.info('Spot data CSV updated from API')
                return dataframe
            else:
                return dataframe


        # Function variables
        end_year    = datetime.now().year-1
        start_year  = end_year-(self.analysis_length-1)
        directory   = os.path.relpath(self.spot_cache_rel_dir)
        filepath    = os.path.join(directory, self.spot_cache_file)

        try:
            # Check if requested data in cached data (CSV file) and read 
            if os.path.exists(filepath):
                dataframe:pd.DataFrame = cached_csv_to_dataframe(filepath)
                # Updates cached data if needed
                dataframe   = update_cache(dataframe)
                # Sets retrieved data to be used for specified analysis length
                start       = f"{start_year}-01-01T00:00:00.000Z"
                end         = f"{end_year}-12-31T23:00:00Z"
                dataframe               = dataframe[start:end]
                # Removes first year if not full year (< 12 months)
                first_year              = dataframe.index.year.min()
                months_in_first_year    = dataframe.loc[dataframe.index.year == first_year].index.month.unique()
                if len(months_in_first_year) != 12:
                    dataframe   = dataframe[dataframe.index.year != dataframe.index.year.min()]
                logging.info('Spot data got from CSV')
                return dataframe
            else:
                start       = f"{start_year}-01-01T00:00:00.000Z"
                end         = f"{end_year}-12-31T23:00:00Z"
                dataframe = from_api_to_dataframe(start, end)
                dataframe.to_csv(filepath)
                first_year              = dataframe.index.year.min()
                months_in_first_year    = dataframe.loc[dataframe.index.year == first_year].index.month.unique()
                if len(months_in_first_year) != 12:
                    dataframe   = dataframe[dataframe.index.year != dataframe.index.year.min()]
                logging.info('Spot data got from API and saved to CSV')
                return dataframe
        except Exception as e:
            logging.error(f"[spot_get_price]: {e}")
            traceback.print_exc()
            return pd.DataFrame

    def spot_pivot_by_hour(self) -> pd.DataFrame:
        """
        Reshapes spot price data by aligning hourly values across multiple years.

        Creates a pivot table where each row represents a specific calendar hour 
        (e.g., "03-15 17:00:00"), and columns represent years. This enables 
        year-over-year hourly comparison.

        Returns:
            pd.DataFrame: Pivoted DataFrame with:
                - Index: Hour key in "%m-%d %H:%M:%S" format.
                - Columns: Years.
                - Values: Spot price in c/kWh.
        """


        # Dictionary to group prices by "MM-DDTHH:MM:SS.000Z"
        try:
            # Define dataframe, check for content
            dataframe   = self.spot_get_price()
            if dataframe.empty:
                logging.error('[spot_pivot_by_hour] Spot price DataFrame is empty')
            # Create column for the hour key: "MM-DDTHH:MM:SS.000Z"
            dataframe['hour_key']   = dataframe.index.strftime("%m-%d %H:%M:%S%z")
            dataframe['year']       = dataframe.index.year
            # Pivot the table: index = hour_key, columns = years, values = price
            dataframe   = dataframe.pivot_table(index='hour_key', columns='year', values='value')
            # Sort by year
            dataframe   = dataframe.sort_index(axis=1)
            return dataframe
        except Exception as e:
            logging.error(f"[spot_pivot_by_hour]: {e}")
            traceback.print_exc()
            return pd.DataFrame

    def spot_calculate_median(self) -> pd.DataFrame:
        """
        Calculates the median spot electricity price for each hour of the year 
        across all years in the dataset.

        Adds two new columns to the pivoted DataFrame:
            - Raw median.
            - Median adjusted with VAT and transfer fees.

        Returns:
            pd.DataFrame: Pivoted DataFrame including:
                - 'Spot median [c/kWh]'
                - 'Spot median w/ fees [c/kWh]'
        """
        try:
            # Define dataframe, check for content
            dataframe   = self.spot_pivot_by_hour()
            if dataframe.empty:
                logging.error('[spot_calculate_median] Spot price DataFrame is empty')
            # Calculate median value for each row
            dataframe[self.spot_price_median]       = dataframe.median(axis=1)
            dataframe[self.spot_price_median_fees]  = (dataframe.median(axis=1) * self.additional_fee)
            return dataframe
        except Exception as e:
            print(f"[spot_calculate_median] Unable to calculate: {e}")
            return pd.DataFrame
    
    def spot_remove_years(self) -> pd.DataFrame:
        """
        Converts the index of the aggregated median data to a single representative year (2024)
        and removes individual year columns.

        Useful for producing a generic hourly profile of median spot prices 
        for a typical year.

        Returns:
            pd.DataFrame: DataFrame indexed by 2024 timestamps (UTC), with:
                - 'Spot median [c/kWh]'
                - 'Spot median w/ fees [c/kWh]'
        """

        try:
            # Define dataframe, check for content
            dataframe   = self.spot_calculate_median()
            if dataframe.empty:
                logging.error('[spot_calculate_median] Spot price DataFrame is empty')
            # Add year to datetime index
            dataframe['date']    = pd.to_datetime('2024-' + dataframe.index, utc=True)
            dataframe.set_index('date', inplace=True)
            # Remove all columns except median and median with fees
            dataframe               = dataframe[[self.spot_price_median, 
                                                 self.spot_price_median_fees]]
            dataframe.columns.name  = None
            return dataframe
        except Exception as e:
            print(f"[spot_remove_years] Error: {e}")
            traceback.print_exc()
            return pd.DataFrame