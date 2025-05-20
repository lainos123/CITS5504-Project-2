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
df['locationId'] = df['Crash ID'].astype(str) + '_' + df['State'] + '_' + df['National Remoteness Areas'] + '_' + df['SA4 Name 2021'] + '_' + df['National LGA Name 2024'] + '_' + df['National Road Type']
df['dateTimeId'] = df['Crash ID'].astype(str) + '_' + df['Month'].astype(str) + '_' + df['Year'].astype(str) + '_' + df['Time']

# Prepare dataframes for each node type
print("Preparing node dataframes...")
# Person nodes
person_df = df[['personId', 'Road User', 'Gender', 'Age', 'Age Group', 'Crash ID']].copy()
person_df.columns = ['personId', 'roadUser', 'gender', 'age', 'ageGroup', 'crashId']
print(f"Created person nodes dataframe with {len(person_df)} records")

# Crash nodes
crash_df = df[['Crash ID', 'Crash Type', 'Number Fatalities', 'Bus Involvement', 
               'Heavy Rigid Truck Involvement', 'Articulated Truck Involvement', 
               'Speed Limit']].copy()
crash_df.columns = ['crashId', 'crashType', 'numberFatalities', 'busInvolvement', 
                    'heavyRigidTruckInvolvement', 'articulatedTruckInvolvement', 'speedLimit']

# Remove duplicates to ensure unique crash nodes
crash_df = crash_df.drop_duplicates(subset=['crashId'])
print(f"Created crash nodes dataframe with {len(crash_df)} unique records")

# Location nodes
location_df = df[['locationId', 'State', 'National Remoteness Areas', 'SA4 Name 2021', 
                  'National LGA Name 2024', 'National Road Type', 'Crash ID']].copy()
location_df.columns = ['locationId', 'state', 'nationalRemoteAreas', 'sa4Name', 
                       'lgaName', 'nationalRoadType', 'crashId']

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

print("Updating location and datetime IDs...")
# update locationId and dateTimeId to remove creashId and then redrop duplicates
location_df['locationId'] = location_df['state'] + '_' + location_df['nationalRemoteAreas'] + '_' + location_df['sa4Name'] + '_' + location_df['lgaName'] + '_' + location_df['nationalRoadType']
dateTime_df['dateTimeId'] = dateTime_df['month'].astype(str) + '_' + dateTime_df['year'].astype(str) + '_' + dateTime_df['time']

# Prepare relationship dataframes
print("Preparing relationship dataframes...")
# For INVOLVED_IN relationship - Person to Crash
person_crash_rel = person_df[['personId', 'crashId']].copy()
print(f"Created person-crash relationships with {len(person_crash_rel)} records")

# For OCCURED_AT relationship - Crash to Location
crash_location_rel = location_df[['crashId', 'locationId']].copy()
print(f"Created crash-location relationships with {len(crash_location_rel)} records")

# For HAPPENED_AT relationship - Crash to DateTime
crash_dateTime_rel = dateTime_df[['crashId', 'dateTimeId']].copy()
print(f"Created crash-datetime relationships with {len(crash_dateTime_rel)} records")

# drop duplicates from location_df and dateTime_df now that we have removed crashId from the locationId and dateTimeId
location_df = location_df.drop_duplicates(subset=['locationId'])
dateTime_df = dateTime_df.drop_duplicates(subset=['dateTimeId'])
print(f"After removing duplicates: {len(location_df)} unique locations and {len(dateTime_df)} unique datetimes")

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
