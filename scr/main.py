import os
from energy_allocator import EnergyAllocator

# Path to energy production file
production_dir      = 'test\profile_production'
production_dir_path = os.path.relpath(production_dir)
production_file     = 'production.csv'
production_path     = os.path.join(production_dir_path, production_file)

# Path to housing company consumption file
company_dir         = 'test\profile_company'
company_dir_path    = os.path.relpath(company_dir)
company_file        = 'company_consumption.csv'
company_path        = os.path.join(company_dir_path, company_file)

# Path to consumption files by apartment type
by_type_dir         = 'test\profiles_by_type'
by_type_dir_path    = os.path.relpath(by_type_dir)
by_type_1_file      = 'A_consumption.csv'
by_type_2_file      = 'B_consumption.csv'
by_type_3_file      = 'C_consumption.csv'
by_type_1_path      = os.path.join(by_type_dir_path, by_type_1_file)
by_type_2_path      = os.path.join(by_type_dir_path, by_type_2_file)
by_type_3_path      = os.path.join(by_type_dir_path, by_type_3_file)

# Path to consumption files by individual apartment
by_apartment_dir      = 'test/profiles_by_apartment'
by_apartment_dir_path = os.path.relpath(by_apartment_dir)
by_apartment_path_A1    = os.path.join(by_apartment_dir_path, 'A1_consumption.csv')
by_apartment_path_A2    = os.path.join(by_apartment_dir_path, 'A2_consumption.csv')
by_apartment_path_A3    = os.path.join(by_apartment_dir_path, 'A3_consumption.csv')
by_apartment_path_A4    = os.path.join(by_apartment_dir_path, 'A4_consumption.csv')
by_apartment_path_B5    = os.path.join(by_apartment_dir_path, 'B5_consumption.csv')
by_apartment_path_B6    = os.path.join(by_apartment_dir_path, 'B6_consumption.csv')
by_apartment_path_B7    = os.path.join(by_apartment_dir_path, 'B7_consumption.csv')
by_apartment_path_C8    = os.path.join(by_apartment_dir_path, 'C8_consumption.csv')
by_apartment_path_C9    = os.path.join(by_apartment_dir_path, 'C9_consumption.csv')
by_apartment_path_C10   = os.path.join(by_apartment_dir_path, 'C10_consumption.csv')
by_apartment_path_C11   = os.path.join(by_apartment_dir_path, 'C11_consumption.csv')


test_data_by_type  = {
    'apartment':    ['A', 'B', 'C',],
    'allocation':   [0.24, 0.27, 0.49,],
    'profile':      [by_type_1_path, by_type_2_path, by_type_3_path],
    'amount':       [4, 3, 4]
}

test_data_by_apartment   = {
    'apartment':    [
                    'A1',                       # 1
                    'A2',                       # 2
                    'A3',                       # 3
                    'A4',                       # 4
                    'B5',                       # 5
                    'B6',                       # 6
                    'B7',                       # 7
                    'C8',                       # 8
                    'C9',                       # 9
                    'C10',                      # 10
                    'C11',                      # 11
                    ],
    'allocation':   [
                    0.06,                       # 1 
                    0.06,                       # 2
                    0.06,                       # 3
                    0.06,                       # 4
                    0.09,                       # 5
                    0.09,                       # 6
                    0.09,                       # 7
                    0.12,                       # 8
                    0.12,                       # 9
                    0.12,                       # 10
                    0.12,                       # 11
                    ],
    'profile':     [
                    by_apartment_path_A1,       # 1
                    by_apartment_path_A2,       # 2
                    by_apartment_path_A3,       # 3
                    by_apartment_path_A4,       # 4
                    by_apartment_path_B5,       # 5
                    by_apartment_path_B6,       # 6
                    by_apartment_path_B7,       # 7
                    by_apartment_path_C8,       # 8
                    by_apartment_path_C9,       # 9
                    by_apartment_path_C10,      # 10
                    by_apartment_path_C11,      # 11
                    ]
}

allocator = EnergyAllocator(
    production_path, 
    company_path, 
    app_data_dict=test_data_by_apartment,
    calculation_method='by_apartment',
    )

enerloc_df  = allocator.sma_value_sum()
print(enerloc_df)
with_energy_community   = enerloc_df.sum()
print("Total value with energy community [€]:", with_energy_community/100)