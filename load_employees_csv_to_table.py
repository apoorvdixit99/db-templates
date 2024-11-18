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

# Path to the CSV file
csv_file_path = "/Users/prathameshdhawale/Desktop/DSCI 551/FDM Project/db-templates/archive/employees.csv"  # Update with the actual path to your file

# Load and insert data from the CSV
with open(csv_file_path, mode='r') as file:
    reader = csv.reader(file)
    headers = next(reader)  # Skip the header row
    placeholders = ", ".join(["%s"] * len(headers))
    columns = ", ".join(headers)
    query = f"INSERT INTO Employees ({columns}) VALUES ({placeholders})"

    for row in reader:
        # Replace empty strings with None for nullable fields
        row = [None if value == '' else value for value in row]
        cursor.execute(query, row)

    connection.commit()
    print("Data successfully inserted into Employees table")

# Close the connection
cursor.close()
connection.close()
