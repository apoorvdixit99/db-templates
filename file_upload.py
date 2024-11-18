from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import pandas as pd
import mysql.connector

app = Flask(__name__)
CORS(app)

UPLOAD_ROOT = "uploads"

# MySQL connection settings
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "password123",
    "database": "userdatabase",  # Replace with your database name
}

# Ensure the database exists
def initialize_database():
    try:
        conn = mysql.connector.connect(
            host=MYSQL_CONFIG["host"],
            user=MYSQL_CONFIG["user"],
            password=MYSQL_CONFIG["password"],
        )
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS dynamic_database;")
        print("Database 'dynamic_database' ensured.")
        conn.close()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        exit(1)  # Exit the script if connection fails


# Clear existing tables in the database
def clear_existing_tables():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    # Get all existing tables in the database
    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()

    # Drop each table
    for table in tables:
        table_name = table[0]
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`;")
        print(f"Table '{table_name}' has been dropped.")

    conn.commit()
    conn.close()


# Process CSV files and insert data into MySQL
def process_csv_to_mysql(file_path, db_type):
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    # Extract table name from file name (without extension)
    table_name = os.path.splitext(os.path.basename(file_path))[0]

    # Read the CSV file
    df = pd.read_csv(file_path)

    # Create a table dynamically based on CSV columns
    columns = ", ".join([f"`{col}` TEXT" for col in df.columns])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS `{table_name}` ({columns});")

    # Insert data into the table
    for _, row in df.iterrows():
        placeholders = ", ".join(["%s"] * len(row))
        sql = f"INSERT INTO `{table_name}` VALUES ({placeholders});"
        cursor.execute(sql, tuple(row))

    conn.commit()
    conn.close()

@app.route("/upload", methods=["POST"])
def upload_files():
    db_type = request.form.get("dbType")
    if not db_type:
        return jsonify({"error": "Database type not provided"}), 400

    if "files" not in request.files:
        return jsonify({"error": "No files found in the request"}), 400

    files = request.files.getlist("files")
    uploaded_files = []

    # Ensure upload folder exists
    db_folder = os.path.join(UPLOAD_ROOT, db_type)
    os.makedirs(db_folder, exist_ok=True)

    if db_type.lower() == "mysql":
        clear_existing_tables()  # Clear existing tables in the database

    for file in files:
        if file.filename == "":
            return jsonify({"error": "One or more files have no filename"}), 400
        file_path = os.path.join(db_folder, file.filename)
        file.save(file_path)
        uploaded_files.append(file_path)

        # Process the file and insert into MySQL if db_type is MySQL
        if db_type.lower() == "mysql":
            process_csv_to_mysql(file_path, db_type)

    return jsonify({
        "message": f"{len(files)} files uploaded and processed successfully!",
        "dbType": db_type,
        "files": uploaded_files
    }), 200

if __name__ == "__main__":
    initialize_database()  # Ensure database is initialized
    app.run(port=3001)
