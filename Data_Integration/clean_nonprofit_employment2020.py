import pandas as pd
import re
 
# State mapping for full names
state_map = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire",
    "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York", "NC": "North Carolina",
    "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania",
    "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee",
    "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming"
}
 
def clean_file(filename):
    try:
        # Read CSV file, skipping first 2 rows (header=2 starts at row 3)
        df = pd.read_csv(filename, low_memory=False, header=2)
 
        # Select necessary columns
        base_columns = df.columns[1:4].to_list()
        nonprofit_columns = [col for col in df.columns if '501(c)(3)' in str(col)]
        additional_columns = df.columns[15:17].to_list()
        columns_to_keep = base_columns + nonprofit_columns + additional_columns
        df = df[columns_to_keep]
 
        # Rename columns
        column_rename_map = {
            'Unnamed: 1': 'Geographic Title',
            'Unnamed: 2': 'NAICS',
            'Unnamed: 3': 'Industry Title',
            '501(c)(3) Nonprofit Establishments': 'Average Establishments',
            '501(c)(3) Nonprofit Establishments.1': 'Annual Average Employment',
            '501(c)(3) Nonprofit Establishments.2': 'Total Annual Wages (in thousands)',
            '501(c)(3) Nonprofit Establishments.3': 'Annual Wages Per Employee',
            '501(c)(3) Nonprofit Establishments.4': 'Average Weekly Wage',
            'Unnamed: 15': 'Percent Employment 501(c)(3)',
            'Unnamed: 16': 'Wage Ratio'
        }
        df.rename(columns=column_rename_map, inplace=True)
 
        # Remove rows with non-numeric NAICS codes
        df['NAICS'] = df['NAICS'].str.strip()  # Clean whitespace
        df = df[df['NAICS'].str.match(r'^\d+$', na=False)]
 
        # Clean numeric columns (remove commas, convert to float)
        numeric_columns = [
            'Average Establishments',
            'Annual Average Employment',
            'Total Annual Wages (in thousands)',
            'Annual Wages Per Employee',
            'Average Weekly Wage',
            'Wage Ratio'
        ]
        for col in numeric_columns:
            df[col] = df[col].str.replace(',', '', regex=True).str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce')
 
        # Clean Percent Employment column
        df['Percent Employment 501(c)(3)'] = df['Percent Employment 501(c)(3)'].str.replace('%', '', regex=False).str.strip()
        df['Percent Employment 501(c)(3)'] = pd.to_numeric(df['Percent Employment 501(c)(3)'], errors='coerce')
 
        # Remove rows with NaN in critical numeric columns
        df.dropna(subset=numeric_columns + ['Percent Employment 501(c)(3)'], inplace=True)
 
        # Remove rows where Geographic Title contains 'U.S. Totals'
        df = df[~df['Geographic Title'].str.contains('U.S. Totals', na=False, case=False)] 

        # Remove multi-state rows (like PA-NJ) and those containing 'MSA'
        df = df[~df['Geographic Title'].str.contains(r',\s*[A-Z]{2}-[A-Z]{2}', na=False)]
        df = df[~df['Geographic Title'].str.contains('MSA', na=False)]
 
        # Convert state abbreviations to full names
        def convert_state(geo_title):
            match = re.search(r',\s*([A-Z]{2})\s*$', geo_title)
            if match:
                state_abbr = match.group(1)
                return geo_title.replace(f", {state_abbr}", f", {state_map.get(state_abbr, state_abbr)}")
            return geo_title
 
        df['Geographic Title'] = df['Geographic Title'].apply(convert_state)
 
        # Identify duplicate rows
        duplicate_rows = df[df.duplicated(subset=['Geographic Title', 'NAICS'], keep=False)]
        if not duplicate_rows.empty:
            print("Duplicate rows detected:")
            print(duplicate_rows)
 
        # Remove duplicates based on key columns
        df = df.drop_duplicates(subset=['Geographic Title', 'NAICS'])

        df = df[df['Geographic Title'] != 'Puerto Rico']
        df = df[df['Geographic Title'] != 'Virgin Islands']

        df = df.iloc[:3037]
 
        return df
 
    except FileNotFoundError:
        print(f"Error: The file '{filename}' was not found.")
        return None
    except KeyError as e:
        print(f"Error: Column {e} not found in the dataset. Check column names.")
        return None
    except ValueError as e:
        print(f"Value error: {e}")
        return None
 
def main():
    """Main function to test the clean_file function."""
    base_path = "/home/norp-services/nccs/data/"
    filename = f"{base_path}qcew-nonprofits-2020.csv"
    cleaned_df = clean_file(filename)
 
    if cleaned_df is not None:
        print("Cleaned DataFrame:")
        print(cleaned_df.head())
 
        # Save cleaned data
        cleaned_df.to_csv(f"{base_path}cleaned_nonprofit_employment_2020.csv", index=False)
        print("\nCleaned data saved as 'cleaned_nonprofit_employment_2020.csv'.")
 
if __name__ == "__main__":
    main()
