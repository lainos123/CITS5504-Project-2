import pandas as pd
import os

# Define input and output paths
input_file = 'data/raw/Project2_Dataset_Corrected.csv'  # Change this to your input CSV file name
output_dir = 'data/neo4j_import'  # Directory to store output CSV files

# Create output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)
print(f"Created output directory: {output_dir}")

# Read the original CSV file
print(f"Reading data from {input_file}...")
df = pd.read_csv(input_file)
print(f"Found {len(df)} records")

# Create unique IDs for nodes
print("Creating unique IDs for nodes...")
df['personId'] = df['ID'].astype(str)  # Using the original ID as personId
# Moved National Road Type out of locationId
df['locationId'] = df['State'] + '_' + df['National Remoteness Areas'] + '_' + df['SA4 Name 2021'] + '_' + df['National LGA Name 2024']
df['dateTimeId'] = df['Month'].astype(str) + '_' + df['Year'].astype(str) + '_' + df['Time']

# Prepare dataframes for each node type
print("Preparing node dataframes...")
# Person nodes
person_df = df[['personId', 'Road User', 'Gender', 'Age', 'Age Group', 'Crash ID']].copy()
person_df.columns = ['personId', 'roadUser', 'gender', 'age', 'ageGroup', 'crashId']
print(f"Created person nodes dataframe with {len(person_df)} records")

# Crash nodes
crash_df = df[['Crash ID', 'Crash Type', 'Number Fatalities', 'Bus Involvement', 
               'Heavy Rigid Truck Involvement', 'Articulated Truck Involvement', 
               'Speed Limit', 'National Road Type']].copy()
crash_df.columns = ['crashId', 'crashType', 'numberFatalities', 'busInvolvement', 
                    'heavyRigidTruckInvolvement', 'articulatedTruckInvolvement', 
                    'speedLimit', 'nationalRoadType']

# Remove duplicates to ensure unique crash nodes
crash_df = crash_df.drop_duplicates(subset=['crashId'])
print(f"Created crash nodes dataframe with {len(crash_df)} unique records")

# Location nodes
location_df = df[['locationId', 'State', 'National Remoteness Areas', 'SA4 Name 2021', 
                  'National LGA Name 2024', 'Crash ID']].copy()
location_df.columns = ['locationId', 'state', 'nationalRemoteAreas', 'sa4Name', 
                       'lgaName', 'crashId']

# Remove duplicates to ensure unique location nodes
location_df = location_df.drop_duplicates(subset=['locationId'])
print(f"Created location nodes dataframe with {len(location_df)} unique records")

# DateTime nodes
dateTime_df = df[['dateTimeId', 'Month', 'Year', 'Dayweek', 'Time', 'Time of day', 
                  'Day of week', 'Christmas Period', 'Easter Period', 'Crash ID']].copy()
dateTime_df.columns = ['dateTimeId', 'month', 'year', 'dayOfWeek', 'time', 'timeOfDay', 
                      'weekdayOrWeekend', 'christmasPeriod', 'easterPeriod', 'crashId']

# Remove duplicates to ensure unique datetime nodes
dateTime_df = dateTime_df.drop_duplicates(subset=['dateTimeId'])
print(f"Created datetime nodes dataframe with {len(dateTime_df)} unique records")

# Prepare relationship dataframes
print("Preparing relationship dataframes...")
# For INVOLVED_IN relationship - Person to Crash
person_crash_rel = person_df[['personId', 'crashId']].copy()
print(f"Created person-crash relationships with {len(person_crash_rel)} records")

# For OCCURED_AT relationship - Crash to Location
crash_location_rel = df[['Crash ID', 'locationId']].copy()
crash_location_rel.columns = ['crashId', 'locationId']
crash_location_rel = crash_location_rel.drop_duplicates()
print(f"Created crash-location relationships with {len(crash_location_rel)} records")

# For HAPPENED_AT relationship - Crash to DateTime
crash_dateTime_rel = df[['Crash ID', 'dateTimeId']].copy()
crash_dateTime_rel.columns = ['crashId', 'dateTimeId']
crash_dateTime_rel = crash_dateTime_rel.drop_duplicates()
print(f"Created crash-datetime relationships with {len(crash_dateTime_rel)} records")

# Export node CSVs
print("Exporting node CSV files...")
person_df.to_csv(f"{output_dir}/person_nodes.csv", index=False)
crash_df.to_csv(f"{output_dir}/crash_nodes.csv", index=False)
location_df.to_csv(f"{output_dir}/location_nodes.csv", index=False)
dateTime_df.to_csv(f"{output_dir}/dateTime_nodes.csv", index=False)

# Export relationship CSVs
print("Exporting relationship CSV files...")
person_crash_rel.to_csv(f"{output_dir}/person_crash_rel.csv", index=False)
crash_location_rel.to_csv(f"{output_dir}/crash_location_rel.csv", index=False)
crash_dateTime_rel.to_csv(f"{output_dir}/crash_dateTime_rel.csv", index=False)
print("All files exported successfully!")