import pandas as pd
import traceback
import time
import logging
logging.basicConfig(level=logging.DEBUG)


class AuxiliaryVar:

    def __init__(self):
    
        self.date_time_formats =    [
            "%m/%d/",               # mm/dd
            "%d/%m/",               # dd/mm
            "%m-%d",                # mm-dd
            "%f/%e/",               # m/d
            "%b %e, ",              # Mon d
            "%B %e, ",              # Month d
            "%m-%d %H:%M:%S",       # mm-dd hh:mm:ss
            "%m-%d %I:%M:%S %p",    # mm-dd hh:mm:ss AM/PM
            "%d %B ",               # dd Month
            "%b %e, ",              # Mon d,
            "%H:%M:%S",             # 24-hour time
            "%I:%M:%S %p",          # 12-hour time with AM/PM
            "%d.%m. %H:%M",         # PVSOL format    

            "%m/%d/%Y",             # mm/dd/yyyy
            "%d/%m/%Y",             # dd/mm/yyyy
            "%Y-%m-%d",             # yyyy-mm-dd
            "%f/%e/%Y",             # m/d/yyyy (platform-specific)
            "%b %e, %Y",            # Mon d, yyyy
            "%B %e, %Y",            # Month d, yyyy
            "%Y-%m-%d %H:%M:%S",    # yyyy-mm-dd hh:mm:ss
            "%Y-%m-%d %I:%M:%S %p", # yyyy-mm-dd hh:mm:ss AM/PM
            "%d %B %Y",             # dd Month yyyy
            "%b %e, %Y",            # Mon d, yyyy
            "%H:%M:%S",             # 24-hour time
            "%I:%M:%S %p",          # 12-hour time with AM/PM
            "%Y-%d.%m. %H:%M",      # PVSOL + year
            "%Y-%m-%d %H:%M:%S%z"   # Pandas DateTime ISO 8601 format with timezone
            ]

class AuxiliaryFunc:
    def __init__(self):
        self.auxiliary_var  = AuxiliaryVar()
    
    def resample_quarter_to_hour(self, dataframe:pd.DataFrame) -> pd.DataFrame:
        """
        Checks if frequency of Pandas DataFrame index is 15 minutes.
        Resamples dataframe to 1 hour index with hourly value being mean
        of 15 minute values.
        Args:
            dataframe (pd.DataFrame): Pandas DataFrame with timestamp index

        Returns:
            pd.DataFrame: DataFrame indexed by hourly timestamps.
        """
        try:
            if pd.infer_freq(dataframe.index) == '15min':
                dataframe = dataframe.resample('1h').mean()
                logging.info("[resample_quarter_to_hour] Quarter hourly data resampled to hourly")
                return dataframe
            else:
                logging.info("[resample_quarter_to_hour] No data in need of resampling detected")
                return dataframe
        except Exception as e:
            logging.error(f"[resample_quarter_to_hour]: {e}")
            logging.error("[resample_quarter_to_hour] Stack trace: \n%s", traceback.print_exc())
            return pd.DataFrame

    def remove_feb_29_if_mismatch(self, df1: pd.DataFrame, df2: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Removes February 29th from one DataFrame if the other contains only 
        8760 hourly rows (non-leap year), to align them for time series operations.

        Args:
            df1 (pd.DataFrame): First time-indexed DataFrame.
            df2 (pd.DataFrame): Second time-indexed DataFrame.

        Returns:
            tuple[pd.DataFrame, pd.DataFrame]: Aligned DataFrames with leap day 
            removed from one if needed.
        """
        try:
            len1, len2 = len(df1), len(df2)

            # Define helper to remove Feb 29
            def remove_feb_29(df: pd.DataFrame) -> pd.DataFrame:
                return df[~((df.index.month == 2) & (df.index.day == 29))]

            # Only remove Feb 29 from one of the DataFrames if lengths mismatch
            if len1 == 8784 and len2 == 8760:
                df1 = remove_feb_29(df1)
            elif len2 == 8784 and len1 == 8760:
                df2 = remove_feb_29(df2)
            elif len1 != len2:
                logging.warning(f"[remove_feb_29_if_mismatch] Length mismatch but unexpected row counts: df1={len1}, df2={len2}")
            return df1, df2
        except Exception as e:
            logging.error(f"[remove_feb_29_if_mismatch]: {e}")
            logging.error("[remove_feb_29_if_mismatch] Stack trace: \n%s", traceback.print_exc())
            return df1, df2
    
    def profile_csv_to_dataframe(self, path:str) -> pd.DataFrame:
        """
        Converts a CSV file to dataframe. Checks if format of first column
        corresponds to a known date-time format. Converts to datetime index
        according to detected format.

        Args:
            path (str): Path to CSV file to convert dataframe

        Returns:
            pd.DataFrame: DataFrame with a DateTime index column and value 
            column

        """
        try:
            dataframe   = pd.read_csv(path, header=None)
        except FileNotFoundError:
            logging.error(f"[profile_csv_to_dataframe]: {path}")
            return pd.DataFrame
        matched_format = None
        try:
            for sample in dataframe[0].dropna().astype(str).head(20):
                for dt_format in self.auxiliary_var.date_time_formats:
                    try:
                       time.strptime(sample.strip(), dt_format)
                       matched_format   = dt_format
                       break
                    except ValueError:
                        continue
                if matched_format:
                    break
            if matched_format:
                if "Y" in  matched_format or "y" in matched_format:
                    dataframe[0]       = pd.to_datetime(
                        dataframe[0], 
                        format=matched_format, 
                        utc=True)
                else:
                    dataframe[0]    = pd.to_datetime(
                                '2024-' + dataframe[0],
                                format=f'%Y-{matched_format}', 
                                utc=True
                                )
                dataframe.set_index(0, inplace=True)
            dataframe   = self.resample_quarter_to_hour(dataframe)
            logging.info("Profile CSV successfully converted to dataframe")
            return dataframe
        except Exception as e:
            logging.error(f"[profile_csv_to_dataframe]: {e}")
            logging.error("[profile_csv_to_dataframe] Stack trace: \n%s", traceback.print_exc())
            return pd.DataFrame