import csv
import pandas as pd
#file_path = "C:/Users/MohammedA(DE-LON16)/Documents/etl-project-1/data/bristol_data.csv"

#with open(file_path,"r") as file:
#    messy_data = file.read()
#    print(messy_data)


file_path = "C:/Users/MohammedA(DE-LON16)/Documents/etl-project-1/chicken-breeder-ETL/data/messy_chicken_data_3.csv"

# 1. load your data

df = pd.read_csv(file_path)

# 2. look at it

#print(df.info())

#STARTING WITH NAMES.
#So we have 13 names that are null. What should we do about it?

missing_names = df[df["name"].isnull()]

df.loc[2,"name"] = "ibrahim"
df.loc[5,"name"] = "xessex"
df.loc[17,"name"] = "nines"
df.loc[31,"name"] = "silky"
df.loc[33,"name"] = "abrahama"
df.loc[36,"name"] = "wingy"
df.loc[43,"name"] = "orap"
df.loc[44,"name"] = "wynnie"
df.loc[48,"name"] = "akanji"
df.loc[51,"name"] = "nani"
df.loc[52,"name"] = "brahimi"
df.loc[58,"name"] = "orpanin"
df.loc[73,"name"] = "isde"
df.loc[76,"name"] = "harpo"
df.loc[78,"name"] = "oprah"
df.loc[90,"name"] = "sussy"
df.loc[94,"name"] = "cochy"

missing_names = df[df["name"].isnull()]
#To check if names is changes in the specific rows.
#print(df.loc[[29,34,81]])
#print(df.info())

# We have 87 no null ages, therfore we have 13 null ages.

#df["birthday"] = pd.to_datetime(df["birthday"], errors="coerce", dayfirst=True)
#print(df.to_string())
missing_birthdays = df[df["birthday"].isnull()]

#df.loc[39,"age"] = ""
#print(missing_birthdays.to_string())
#print(df.info())
# There's 5 missing birthdays, we will remove them. Locations 20,21,27,46,54

df = df[df["birthday"].notna()]
missing_birthdays = df[df["birthday"].isnull()]

# Now we will check how many breeds are missing. Since this information is something we need, if its missing we will remove the entire row.

missing_breeds = df[df["breed"].isnull()]

# 7 chickens are misiing breeds. The .notna() will remove any rows with missing breeds
df = df[df["breed"].notna()]
missing_breeds = df[df["breed"].isnull()]

df = df[df["age"].notna()]



# Add this after your existing cleaning steps in app.py
def clean_size_column(df):
    df_clean = df.copy()
    df_clean['size'] = df_clean['size'].astype(str).str.lower().str.strip()
    
    size_mapping = {
        'vs': 'Very Small', 'very small': 'Very Small', 'tiny': 'Very Small', 'tinyy': 'Very Small',
        's': 'Small', 'small': 'Small', 'smal': 'Small',
        'm': 'Medium', 'med': 'Medium', 'medium': 'Medium',
        'l': 'Large', 'large': 'Large',
        'vl': 'Very Large', 'very large': 'Very Large', 'veri large': 'Very Large', 'huge': 'Very Large',
        'nan': None, '': None
    }
    
    df_clean['size'] = df_clean['size'].map(size_mapping)
    return df_clean

# Apply it in your pipeline
df = clean_size_column(df)

missing_sizes = df[df["size"].isnull()]
df = df[df["size"].notna()]
missing_sizes = df[df["size"].isnull()]


def clean_age_column(df):
    df_clean = df.copy()
    
    # Step 1: Convert to string and clean
    df_clean["age"] = df_clean["age"].astype(str).str.lower().str.strip()
    
    # Step 2: Replace specific text values
    age_mapping = {
        "ten": "10",
        "unknown": None,
        "nan": None  # Handle pandas NaN values converted to string
    }
    
    df_clean["age"] = df_clean["age"].replace(age_mapping)
    
    # Step 3: Convert to numeric (handles all normal numbers)
    df_clean["age"] = pd.to_numeric(df_clean["age"], errors='coerce')
    
    # Step 4: Handle negative ages
    df_clean.loc[df_clean["age"] < 0, "age"] = None
    
    # Step 5: Remove rows with missing ages
    df_clean = df_clean.dropna(subset=["age"])
    
    # Step 6: Convert to integer
    df_clean["age"] = df_clean["age"].astype(int)
    
    return df_clean

# Usage:
df = clean_age_column(df)
print(df.info())