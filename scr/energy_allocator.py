"""
TODO: Implement SMA and SMB compensation models.
"""

import pandas as pd
import traceback
from auxiliary_module import AuxiliaryVar, AuxiliaryFunc
from spot_module import SpotMedianCalculator
import logging
logging.basicConfig(level=logging.WARNING)

class EnergyAllocator:
    """
    Performs energy allocation analysis for a housing company using photovoltaic (PV) production data.
    Calculates how much energy is:
    - Allocated to the housing company and its apartments.
    - Saved in monetary value through self-consumption.
    - Leftover and sold back to the electricity grid.

    Attributes:
        production_path (str): Path to the CSV file containing hourly PV energy production data.
        company_path (str): Path to the CSV file with the housing company's hourly energy consumption.
        app_data_dict (dict[str, list]): Dictionary describing apartment data. Each entry contains:
            - "apartment" (str): Apartment name or type.
            - "allocation" (float): Fraction of remaining PV energy allocated.
            - "profile" (str): CSV path to apartment's hourly consumption profile.
            - "amount" (int, optional): Number of apartments of this type (used for `by_type` mode).
        calculation_method (str): "by_apartment" or "by_type" indicating how allocation is handled.
        analysis_length (int): Number of years of spot price data used for median calculation (max: 13).
        vat_perc (float): VAT percentage (as a decimal) to include in value calculations.
        transfer_fee_perc (float): Transfer fee per kWh to include in value calculations.
        spot_cache_rel_dir (str): Directory where spot price statistics are cached.
        spot_cache_file (str): Filename of cached spot price data.
    """

# CLASS CONSTANTS ------------------------------------------------------------
    def __init__(
            self,
            production_path         :str                = None,
            company_path            :str                = None,
            app_data_dict           :dict[str:list]     = None,
            calculation_method      :str                ="by_apartment", # Alternative: "by_type"
            analysis_length         :int                = 5,
            vat_perc                :float              = 0.255,
            transfer_fee_perc       :float              = 0.111,
            spot_cache_rel_dir      :str                = "assets",
            spot_cache_file         :str                = "spot_price_data.csv",

    ):
        # Analysis length limit
        if analysis_length > 13:
            analysis_length = 13

        self.production_path        = production_path
        self.company_path           = company_path
        self.app_data_dict          = app_data_dict
        self.calculation_method     = calculation_method
        self.auxiliary_var          = AuxiliaryVar()
        self.auxiliary_func         = AuxiliaryFunc()
        self.spot_processor         = SpotMedianCalculator(
                                                            analysis_length, 
                                                            vat_perc, 
                                                            transfer_fee_perc, 
                                                            spot_cache_rel_dir, 
                                                            spot_cache_file
                                                            )

        self.spot_price_median              = "Spot median [c/kWh]"
        self.spot_price_median_fees         = "Spot median w/ fees [c/kWh]"

        self.production_column              = "PV production [kWh]"
        self.company_column                 = "Company consumption [kWh]"
        self.company_after_pv               = "Company after PV [kWh]"
        self.value_after_subtraction        = "Company value of coverage [c]"
        self.pv_after_company               = "PV post company [kWh]"

        self.temp_apartment_consumption     = "APP consumption [kWh]"
        self.temp_app_after_pv              = "APP after PV [kWh]"
        self.temp_value_of_coverage         = "APP value of coverage [c]"

        self.pv_over_production             = "PV over production [kWh]"
        self.value_to_grid                  = "Electricity value to grid [c]"

# PROFILE PROCESSING FUNCTIONS -----------------------------------------------
    def add_production(self) -> pd.DataFrame:
        """
        Adds PV production data to the spot price DataFrame.

        Steps:
        - Loads hourly PV production from CSV.
        - Aligns it with the hourly index used for spot prices.
        - Handles leap year inconsistencies.
        - Adds a new column: "PV production [kWh]".

        Returns:
            pd.DataFrame: DataFrame indexed by datetime with PV production added.
        """


        try:
            self.production_df  = self.auxiliary_func.profile_csv_to_dataframe(self.production_path)
            self.production_df.columns = [self.production_column]
            dataframe = self.spot_processor.spot_remove_years()
            # Ensure datetime index for both dataframes
            if not isinstance(self.production_df.index, pd.DatetimeIndex):
                self.production_df.index   = pd.to_datetime(
                    self.production_df.index, utc=True)
            # Drop Feb 29 if one dataframe has 8760 hourse (leap year mismatch)
            self.production_df, dataframe = self.auxiliary_func.remove_feb_29_if_mismatch(self.production_df, dataframe)
            # Check for specified column in dataframe
            if self.production_column not in self.production_df.columns:
                raise KeyError(f"Column {self.production_column} not found in production data.")
            # Align PV production index to match dataframe
            self.production_df = self.production_df.reindex(dataframe.index)
            # Check for NaN values in value or index columns respectively
            if self.production_df[self.production_column].isna().any():
                logging.warning("[add_production]: NaN values after reindexing production data.")
            if not self.production_df.index.difference(dataframe.index).empty:
                logging.warning("[add_production] Warning: Production index partially mismatched with price index.")
            # Assign aligned PV production to new column
            dataframe[self.production_column] = self.production_df[self.production_column]
            return dataframe
        except Exception as e:
            logging.error(f"[add_production] Error : {e}")
            logging.error("[add_production] Stack trace: \n%s", traceback.print_exc())
            return pd.DataFrame()

    def add_company_consumption(self) -> pd.DataFrame:
        """
        Adds housing company's consumption profile to the analysis DataFrame.

        Steps:
        - Loads CSV data of company consumption.
        - Aligns it with the same hourly index used for PV and spot prices.
        - Adds a new column: "Company consumption [kWh]".

        Returns:
            pd.DataFrame: DataFrame with housing company consumption data appended.
        """


        try:
            company_df  = self.auxiliary_func.profile_csv_to_dataframe(self.company_path)
            company_df.columns = [self.company_column]
            # Determine function to use as basis for dataframe
            dataframe = self.add_production()
            # Ensure datetime index for both dataframes
            if not isinstance(company_df.index, pd.DatetimeIndex):
                company_df.index = pd.to_datetime(company_df.index, utc=True)
            # Drop Feb 29 if one dataframe has 8760 hourse (leap year mismatch)
            company_df, dataframe = self.auxiliary_func.remove_feb_29_if_mismatch(company_df, dataframe)
            # Check for specified column in dataframe
            if self.company_column not in company_df.columns:
                raise KeyError(f"Column {self.company_column} not found in production data.")
            # Align company_df consumption index to match dataframe
            company_df = company_df.reindex(dataframe.index)
            # Check for NaN values in value or index columns respectively
            if company_df[self.company_column].isna().any():
                logging.warning("[add_company_consumption]: NaN values after reindexing company_df consumption data.")
            if company_df.index.difference(dataframe.index).empty:
                logging.warning("[add_production] Consumption index partially mismatched with price index.")
            # Assign aligned company_df consumption to new column
            dataframe[self.company_column] = company_df[self.company_column]
            return dataframe
        except Exception as e:
            logging.error(f"[add_company_consumption]: {e}")
            logging.error("[add_company_consumption] Stack trace: \n%s", traceback.print_exc())
            return pd.DataFrame()
            
    def calculate_company(self) -> pd.DataFrame:
        """
        Calculates PV energy usage and savings for the housing company.

        Adds:
        - "Company after PV [kWh]": Remaining unmet consumption.
        - "Company value of coverage [c]": Cost savings from covered energy.
        - "PV post company [kWh]": Excess PV energy after company consumption.

        Returns:
            pd.DataFrame: DataFrame with energy balance and financial value columns.
        """


        try:
            dataframe               = self.add_company_consumption()
            # Safety check
            required_columns = [
                self.company_column, self.production_column, self.spot_price_median
            ]
            for col in required_columns:
                if col not in dataframe.columns:
                    raise KeyError(f"Column {col} missing from dataframe.")
            # Energy coverage calculation
            unmet_consumption = dataframe[self.company_column] - dataframe[self.production_column]
            dataframe[self.company_after_pv] = unmet_consumption.clip(lower=0)
            # Financial value calculation
            covered_consumption = dataframe[self.company_column] - dataframe[self.company_after_pv]
            value = (dataframe[self.spot_price_median_fees]) * covered_consumption
            dataframe[self.value_after_subtraction]  = value
            # Production leftover calculation
            excess_pv = dataframe[self.production_column] - dataframe[self.company_column]
            dataframe[self.pv_after_company] = excess_pv.clip(lower=0)
            return dataframe
        except Exception as e:
            logging.error(f"[calculate_company]: {e}")
            logging.error("[calculate_company] Stack trace: \n%s", traceback.print_exc())
            return pd.DataFrame()
    
    def add_apartment_consumption(self) -> pd.DataFrame:
        """
        Adds apartment consumption data to the analysis DataFrame.

        Steps:
        - Reads hourly consumption profiles for each apartment or apartment type.
        - Normalizes allocation weights if using `by_type`.
        - Adds consumption columns for each apartment in the form:
            "<Apartment> consumption [kWh]"

        Returns:
            pd.DataFrame: DataFrame with apartment consumption profiles appended.
        """

        try:
            dataframe   = self.calculate_company()
            self.profile_df = pd.DataFrame()
            # Method to take when calculating energy by apartment
            if self.calculation_method == "by_apartment":
                self.apartments_df  = pd.DataFrame(self.app_data_dict).set_index("apartment")
            # Method to take when calculating energy by apartment type
            elif self.calculation_method == "by_type":
                self.apartments_df  = pd.DataFrame(self.app_data_dict).set_index("apartment")
                dataframe   = self.calculate_company()
                # Repeat each row by row in "amount" column
                self.apartments_df = self.apartments_df.loc[self.apartments_df.index.repeat(self.apartments_df["amount"])].copy()
                # Store the apartment group letter in a new column
                self.apartments_df["apartment_letter"] = self.apartments_df.index
                # Create a global counter starting from 1
                global_index = range(1, len(self.apartments_df) + 1)
                # Set new index like A1, A2, ..., C11
                self.apartments_df.index = [f"{apt}{i}" for apt, i in zip(self.apartments_df["apartment_letter"], global_index)]
                # Drop helper column
                self.apartments_df.drop(columns="apartment_letter", inplace=True)
                # Normalize allocation across duplicated rows
                self.apartments_df["allocation"] = self.apartments_df["allocation"] / self.apartments_df["amount"]
            for apartment in self.apartments_df.index:
                self.apartment_consumption  = self.temp_apartment_consumption.replace("APP", apartment)
                path    = self.apartments_df.loc[apartment, "profile"]
                self.profile_df[self.apartment_consumption]   = self.auxiliary_func.profile_csv_to_dataframe(path)
                self.auxiliary_func.remove_feb_29_if_mismatch(dataframe, self.profile_df)
                self.apartment_consumption  = self.temp_apartment_consumption.replace("APP", apartment)
                dataframe[self.apartment_consumption] = self.profile_df[self.apartment_consumption]
            return dataframe           
        except Exception as e:
            logging.error(f"[add_apartment_consumption]: {e}")
            logging.error("[add_apartment_consumption] Stack trace: \n%s", traceback.print_exc())
            return pd.DataFrame()

    def calculate_apartment(self) -> pd.DataFrame:
        """
        Calculates PV coverage and monetary value for each apartment.

        Adds, per apartment:
        - "<Apartment> after PV [kWh]": Remaining unmet consumption.
        - "<Apartment> value of coverage [c]": Financial value of PV coverage.

        Returns:
            pd.DataFrame: DataFrame with apartment-specific energy and value metrics.
        """


        try:
            dataframe   = self.add_apartment_consumption()
            for apartment in self.apartments_df.index:
                apartment_column = f"{apartment} consumption [kWh]"
                if apartment_column not in dataframe.columns:
                    raise KeyError(f"Expected column {apartment_column} not found.")
                self.app_after_pv       = self.temp_app_after_pv.replace("APP", apartment)
                self.value_of_coverage  = self.temp_value_of_coverage.replace("APP", apartment)

                app_after_pv    =   dataframe[apartment_column] - (dataframe[self.pv_after_company]*self.apartments_df.loc[apartment, "allocation"])
                covered_consumption = dataframe[apartment_column] - app_after_pv
                allocation_value = (dataframe[self.spot_price_median_fees]) * covered_consumption
                insert_loc = dataframe.columns.get_loc(apartment_column) + 1

                dataframe.insert(insert_loc, self.app_after_pv, app_after_pv)
                dataframe.insert(insert_loc + 1, self.value_of_coverage, allocation_value)
            return dataframe
        except Exception as e:
            logging.error(f"[calculate_apartment]: {e}")
            logging.error("[calculate_apartment] Stack trace: \n%s", traceback.print_exc())
            return pd.DataFrame()

    def calculate_pv_over_production(self) -> pd.DataFrame:
        """
        Calculates PV energy not used by the company or apartments,
        and its financial value from grid sales.

        Adds:
        - "PV over production [kWh]": Surplus PV energy.
        - "Electricity value to grid [c]": Value of sold energy at spot price.

        Returns:
            pd.DataFrame: DataFrame including surplus energy and value columns.
        """


        try:
            dataframe   = self.calculate_apartment()
            # Calculate energy left after consumption coverage with < 0 = 0
            calculation = (dataframe[self.production_column] 
                           - (dataframe[self.company_column] 
                              + self.profile_df.sum(axis=1))).clip(lower=0)
            dataframe[self.pv_over_production] = calculation
            value   = dataframe[self.pv_over_production] * dataframe[self.spot_price_median]
            dataframe[self.value_to_grid]   = value
            return dataframe.round(3)
        except Exception as e:
            logging.error(f"[calculate_pv_over_production]: {e}")
            traceback.print_exc()
            return pd.DataFrame()
        
    def sma_value_sum(self) -> pd.Series:  
        """
        Calculates total monetary value (in cents) from PV energy use.

        Includes:
        - Savings for the housing company.
        - Savings for apartments.
        - Revenue from excess energy sold to the grid.

        Returns:
            pd.Series: Total monetary values per category in cents.
        """


        try:
            dataframe = self.calculate_pv_over_production()
            value_columns        = [col for col in dataframe.columns if "value" in col.lower()]
            return dataframe[value_columns].sum().round(0)
        except Exception as e:
            logging.error(f"[financial_value_sum]: {e}")
            logging.error("[financial_value_sum] Stack trace: \n%s", traceback.print_exc())
            return pd.DataFrame()
        
    def energy_value_sum(self) -> pd.Series:  
        """
        Calculates total energy covered (in kWh) for each entity.
        
        Calculates the difference between "consumption" and "after PV" for:
        - Housing company
        - Each apartment
        
        Returns:
            pd.Series: Total kWh covered for each apartment and the company.
        """


        try:
            dataframe = self.calculate_pv_over_production()
            before_columns = [col for col in dataframe.columns if "consumption" in col.lower()]
            after_columns = [col for col in dataframe.columns if "after" in col.lower()]
            if len(before_columns) != len(after_columns):
                raise ValueError("Mismatched number of 'before' and 'after' columns")
            # Compute differences for each pair
            savings = {}
            for before, after in zip(before_columns, after_columns):
                savings[before.replace("consumption", "consumption covered")] = (dataframe[before] - dataframe[after]).sum()
            return pd.Series(savings)
        except Exception as e:
            logging.error(f"[energy_value_sum]: {e}")
            logging.error("[energy_value_sum] Stack trace: \n%s", traceback.print_exc())
            return pd.DataFrame()