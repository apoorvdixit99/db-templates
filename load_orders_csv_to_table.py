import mysql.connector
import csv
import os
import pandas as pd

# Database connection
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root123",
    database="NorthWind"
)
cursor = connection.cursor()

# Folder containing CSV files
csv_folder = "/Users/prathameshdhawale/Desktop/DSCI 551/FDM Project/archive"

# Mapping of CSV files to tables
csv_to_table = {
    "orders.csv": "Orders"
}

# Function to clean row data for NULL values
def clean_row(row, headers):
    cleaned_row = []
    for i, value in enumerate(row):
        # Replace empty strings with None for nullable fields
        if headers[i].lower() in ['shippeddate'] and value.strip() == '':
            cleaned_row.append(None)
        else:
            cleaned_row.append(value)
    return cleaned_row

# Load each CSV file
for csv_file, table_name in csv_to_table.items():
    file_path = os.path.join(csv_folder, csv_file)

    # Load CSV data into a DataFrame for preprocessing
    df = pd.read_csv(file_path)
    
    # Handle null or empty values in the 'shippeddate' column
    if 'shippeddate' in df.columns:
        df['shippeddate'] = df['shippeddate'].replace(['', None], pd.NA)

    # Save the cleaned data to a temporary CSV file
    cleaned_file_path = os.path.join(csv_folder, f"cleaned_{csv_file}")
    df.to_csv(cleaned_file_path, index=False)

    # Read and load the cleaned data
    with open(cleaned_file_path, mode='r') as file:
        reader = csv.reader(file)
        headers = next(reader)  # Skip the header row
        placeholders = ", ".join(["%s"] * len(headers))
        columns = ", ".join(headers)
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        for row in reader:
            cleaned_row = clean_row(row, headers)
            cursor.execute(query, cleaned_row)
        connection.commit()
        print(f"Inserted data from {csv_file} into {table_name}")

# Close the connection
cursor.close()
connection.close()
