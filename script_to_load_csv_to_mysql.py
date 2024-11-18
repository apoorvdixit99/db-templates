import mysql.connector
import csv
import os

# Database connection
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root123",
    database="NorthWind"
)
cursor = connection.cursor()

# Folder containing CSV files
csv_folder = "/Users/prathameshdhawale/Desktop/DSCI 551/FDM Project/db-templates/archive"

# Mapping of CSV files to tables
csv_to_table = {
    "employees.csv": "Employees"
}

# Load each CSV file
for csv_file, table_name in csv_to_table.items():
    file_path = os.path.join(csv_folder, csv_file)
    with open(file_path, mode='r') as file:
        reader = csv.reader(file)
        headers = next(reader)  # Skip the header row
        placeholders = ", ".join(["%s"] * len(headers))
        columns = ", ".join(headers)
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        for row in reader:
            cursor.execute(query, row)
        connection.commit()
        print(f"Inserted data from {csv_file} into {table_name}")

# Close the connection
cursor.close()
connection.close()
