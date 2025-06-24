# **Energy Allocation and Financial Analysis System**

## **Overview**

The **Energy Allocation and Financial Analysis System** is a Python-based 
program designed to simulate and calculate energy allocation between a housing 
company and its apartments based on own energy production (typically wind or 
solar power). The system takes into account the energy consumed by the 
company and individual apartments, alongside their respective consumption 
profiles. It also calculates the financial savings resulting from PV energy 
usage, as well as the value of excess energy sold back to the grid.

The primary goal of this system is to optimize the energy usage of a housing 
company, by determining how much energy each apartment can consume based on 
energy production and financial savings through electricity spot market rates.

---

## **Key Features**

* **Energy Allocation**:  The system calculates how much energy each apartment 
and the company should receive based on their respective consumption profiles.
* **Financial Analysis**: Calculates the monetary savings for the housing 
company and its apartments from using produced energy and sells any excess 
production to the grid at a spot price.
* **Flexibility**: The program can handle different consumption profiles by 
apartment type or by individual apartment, allowing for different allocation 
schemes.
* **Data Integration**: It integrates data from CSV files containing energy 
consumption profiles, PV production, and electricity spot prices.
* **Optimization**: The system provides an efficient means to analyze and 
optimize energy allocation, accounting for overproduction and energy trading.

---

## **Modules and Classes**

### 1. **`EnergyAllocator`** (in `energy_allocator.py`)

* The main class responsible for handling the energy allocation calculations 
and financial analysis. It integrates data from the consumption profiles and 
the PV production data to determine energy allocation for both the company 
and individual apartments.
* Key Methods:

  * `sma_value_sum()`: Calculates and returns the total financial savings and 
  value of energy for both the company and its apartments.

### 2. **`AuxiliaryVar` and `AuxiliaryFunc`** (in `auxiliary_module.py`)

* Helper classes providing utility functions for common tasks such as:

  * Reading and processing CSV files.
  * Aligning time series data (e.g., removing leap days if the time series 
  are misaligned).

### 3. **`SpotMedianCalculator`** (in `spot_module.py`)

* Responsible for calculating the median electricity spot price over a given 
time period. This data is used to determine the financial value of PV energy 
savings.

### 4. **Test Data Files**

* Contains consumption profiles for the company and apartments, as well as 
the energy production data.
* The test data is used for running the energy allocation calculations and 
for testing the functionality of the system.

---

## **Getting Started**

### Prerequisites

To run this program, the following Python packages are required:

* `pandas` (for data manipulation)
* `numpy` (for numerical calculations)
* `os` (for file management)

The above dependencies can be installed by running the following command:

```
pip install pandas numpy
```

### Installation

1. Clone this repository to local directory:

   ```
   git clone https://github.com/GeorgyTsvetkov/energy-allocator.git
   ```

2. Navigate to the project directory:

   ```
   cd energy-allocation
   ```

3. Ensure all necessary CSV files are present in the 
`test/profile_production`, `test/profile_company`, and 
`test/profiles_by_type` directories, as these files contain the production 
and consumption data used by the system.

---

## **How It Works**

1. **Energy Allocation**:

   * The program uses the provided production data (`production.csv`) and 
   company consumption data (`company_consumption.csv`) to allocate produced 
   energy to both the housing company and individual apartments.
   * The consumption profiles for each apartment are specified in the 
   `test_data_by_type` or `test_data_by_apartment` dictionaries. The system 
   adjusts the energy allocation based on the defined allocation percentages.

2. **Financial Calculation**:

   * The system uses spot market data to calculate the monetary value of the 
   energy consumed by the company and apartments, as well as the value of any 
   excess energy sold back to the grid. The `SpotMedianCalculator` class 
   provides the necessary data for these calculations.

3. **Report Generation**:

   * After performing the calculations, the program generates a summary 
   report of the total energy usage, savings, and the value of excess energy. 
   This report is printed to the console, but can also be saved to a CSV file 
   for further analysis.

4. **Test Data**:

   * For ease of testing, the program includes predefined test data in the 
   `test_data_by_type` and `test_data_by_apartment` dictionaries, as well as 
   sample consumption and production profiles. These files can be modified for 
   purposes of running different scenarios.

---

## **Usage**

1. **Running the Program**:

   * Once the dependencies are installed and the CSV files are in place, 
   the main program can be run by executing the `energy_allocator.py` script.
   * To run the script with a test case, use the following command:

     ```
     python energy_allocator.py
     ```

2. **Configuring the Input Data**:

   * Modify the paths in the script to point to the correct locations of the 
   CSV files containing the energy production and consumption data.
   * The `test_data_by_type` and `test_data_by_apartment` 
   dictionaries can be adjusted to match own data if necessary.

3. **Output**:

   * The output will be a summary of energy usage and financial savings, 
   printed to the console.

---

## **Example Output**

```
Total value with energy community [€]: 150.45
```

This output represents the total monetary savings for the company and 
apartments, as well as the value of any excess energy sold to the grid.

---

## **License**

This project is licensed under the MIT License - see 
[LICENSE](https://choosealicense.com/licenses/mit/)for details.

---

## **Acknowledgments**

* This program uses open-source Python libraries like `pandas` and `numpy` 
for data manipulation and calculations.
* Spot market data and consumption profiles are provided as input and can 
be customized based on real-world data.
