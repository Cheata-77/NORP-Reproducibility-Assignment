import pandas as pd

# Dictionary mapping state abbreviations to full names
STATE_ABBREVIATIONS = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
    'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
    'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri',
    'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
    'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio',
    'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont',
    'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming',
    'DC': 'District of Columbia', 'PR': 'Puerto Rico'
}

def clean_file(filename, ein_mapping_file):
    """Reads a CSV file, keeps selected columns, renames them, 
    and adds EIN location mapping from another file."""
    try:
        # Read the main dataset
        df = pd.read_csv(filename, low_memory=False)

        # Columns to keep
        columns_to_keep = ['F9_00_ORG_EIN', 'F9_09_EXP_OTH_EMPL_BEN_TOT', 'F9_09_EXP_OTH_SAL_WAGE_TOT',
                           'F9_08_REV_TOT_TOT', 'F9_08_REV_OTH_FUNDR_DIRECT_EXP',
                           'F9_08_REV_CONTR_TOT', 'F9_05_NUM_EMPL']

        # Keep only the selected columns
        df = df[columns_to_keep]

        # Rename columns
        column_rename_map = {
            'F9_00_ORG_EIN': 'Employee Identification Number',
            'F9_09_EXP_OTH_EMPL_BEN_TOT': 'Other Employee Benefit',
            'F9_09_EXP_OTH_SAL_WAGE_TOT': 'Other Salaries and Wages - Total',
            'F9_08_REV_TOT_TOT': 'Total Revenue',
            'F9_08_REV_OTH_FUNDR_DIRECT_EXP': 'Direct Expenses',
            'F9_08_REV_CONTR_TOT': 'Total Contributions',
            'F9_05_NUM_EMPL': 'Number of Employees'
        }
        df.rename(columns=column_rename_map, inplace=True)

        # Read the EIN-State mapping file
        df_ein = pd.read_csv(ein_mapping_file, low_memory=False)

        # Convert EIN columns to string for consistent merging
        df["Employee Identification Number"] = df["Employee Identification Number"].astype(str)
        df_ein["Ein"] = df_ein["Ein"].astype(str)

        # Merge the datasets to add the state column
        df = df.merge(df_ein[["Ein", "State"]], left_on="Employee Identification Number", right_on="Ein", how="left")

        # Drop the redundant EIN column from the merged dataframe
        df.drop(columns=["Ein"], inplace=True)

        # Remove rows where State is missing (i.e., EIN not found in mapping)
        df = df.dropna(subset=["State"])

        # Convert state abbreviations to full names
        df["State"] = df["State"].map(STATE_ABBREVIATIONS).fillna(df["State"])  # Keeps original value if not found

        # Remove duplicate EINs and keep first occurrence
        df = df.drop_duplicates(subset=["Employee Identification Number"], keep="first")
        
        df = df[df['State'] != 'Puerto Rico']

        df = df[df['State'] != 'VI']
        return df
    
    except FileNotFoundError:
        print(f"Error: The file '{filename}' was not found.")
        return None
    except KeyError as e:
        print(f"Error: Column {e} not found in the dataset. Check column names.")
        return None

def main():
    """Main function to test the clean_file function."""
    base_path = "/home/norp-services/nccs/data/"
    filename = f"{base_path}CORE-2020-501C3-CHARITIES-PC-HRMN.csv"  # Ensure this file exists
    ein_mapping_file = f"{base_path}irs_990_rev_trends.csv"  # Ensure this file exists

    # Call the cleaning function
    cleaned_df = clean_file(filename, ein_mapping_file)

    if cleaned_df is not None:
        # Display the cleaned DataFrame
        print("Cleaned DataFrame:")
        print(cleaned_df.head())  # Show first few rows

        # Save cleaned data to a new file
        cleaned_df.to_csv(f"{base_path}cleaned_charities_2020.csv", index=False)
        print("\nCleaned data saved as 'cleaned_charities_2020.csv'.")

if __name__ == "__main__":
    main()

