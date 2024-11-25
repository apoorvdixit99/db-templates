# from flask import Flask, request, jsonify
# from flask_cors import CORS
# import os
# import pandas as pd
# import mysql.connector
# from pymongo import MongoClient
# from config import MYSQL_CONFIG, MONGO_URI
# import logging

# # Set up logging
# logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# app = Flask(__name__)
# CORS(app)

# UPLOAD_ROOT = "uploads"

# # Ensure the MySQL database exists
# def initialize_mysql_database():
#     try:
#         conn = mysql.connector.connect(
#             host=MYSQL_CONFIG["host"],
#             user=MYSQL_CONFIG["user"],
#             password=MYSQL_CONFIG["password"],
#         )
#         cursor = conn.cursor()
#         cursor.execute("CREATE DATABASE IF NOT EXISTS userdatabase;")
#         logging.info("MySQL Database 'userdatabase' ensured.")
#         conn.close()
#     except mysql.connector.Error as err:
#         logging.error(f"MySQL Error during initialization: {err}")
#         exit(1)  # Exit the script if connection fails

# # Clear existing tables in the MySQL database
# def clear_existing_mysql_tables():
#     try:
#         conn = mysql.connector.connect(**MYSQL_CONFIG)
#         cursor = conn.cursor()

#         # Get all existing tables in the database
#         cursor.execute("SHOW TABLES;")
#         tables = cursor.fetchall()

#         # Drop each table
#         for table in tables:
#             table_name = table[0]
#             cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`;")
#             logging.info(f"MySQL Table '{table_name}' has been dropped.")

#         conn.commit()
#         conn.close()
#     except Exception as e:
#         logging.error(f"Error clearing MySQL tables: {e}")
# # 
# # Process CSV files and insert data into MySQL
# def process_csv_to_mysql(file_path):
#     try:
#         conn = mysql.connector.connect(**MYSQL_CONFIG)
#         cursor = conn.cursor()

#         # Extract table name from file name (without extension)
#         table_name = os.path.splitext(os.path.basename(file_path))[0]
#         logging.debug(f"Processing MySQL table '{table_name}'.")

#         # Read the CSV file
#         df = pd.read_csv(file_path)
#         logging.debug(f"CSV columns for '{table_name}': {df.columns.tolist()}")

#         # Create a table dynamically based on CSV columns
#         columns = ", ".join([f"`{col}` TEXT" for col in df.columns])
#         cursor.execute(f"CREATE TABLE IF NOT EXISTS `{table_name}` ({columns});")

#         # Insert data into the table
#         for _, row in df.iterrows():
#             placeholders = ", ".join(["%s"] * len(row))
#             sql = f"INSERT INTO `{table_name}` VALUES ({placeholders});"
#             cursor.execute(sql, tuple(row))

#         conn.commit()
#         conn.close()
#         logging.info(f"MySQL Table '{table_name}' created and data inserted successfully.")
#     except Exception as e:
#         logging.error(f"Error processing MySQL file '{file_path}': {e}")

# # Process CSV files and insert data into MongoDB
# def process_csv_to_mongodb(file_path):
#     try:
#         client = MongoClient(MONGO_URI)
#         db = client["userdatabase"]  # Replace with your MongoDB database name

#         # Extract collection name from file name (without extension)
#         collection_name = os.path.splitext(os.path.basename(file_path))[0]
#         logging.debug(f"Processing MongoDB collection '{collection_name}'.")

#         # Drop the existing collection if it exists
#         if collection_name in db.list_collection_names():
#             db[collection_name].drop()
#             logging.info(f"MongoDB Collection '{collection_name}' dropped.")

#         # Read the CSV file
#         df = pd.read_csv(file_path)
#         logging.debug(f"CSV columns for MongoDB '{collection_name}': {df.columns.tolist()}")

#         # Insert data into the collection
#         data = df.to_dict(orient="records")  # Convert DataFrame to list of dicts
#         collection = db[collection_name]
#         collection.insert_many(data)
#         logging.info(f"Data inserted into MongoDB Collection '{collection_name}'.")
#         client.close()
#     except Exception as e:
#         logging.error(f"Error processing MongoDB file '{file_path}': {e}")

# @app.route("/upload", methods=["POST"])
# def upload_files():
#     try:
#         db_type = request.form.get("dbType")
#         print(db_type)
#         if not db_type:
#             logging.warning("Database type not provided.")
#             return jsonify({"error": "Database type not provided"}), 400

#         if "files" not in request.files:
#             logging.warning("No files found in the request.")
#             return jsonify({"error": "No files found in the request"}), 400

#         files = request.files.getlist("files")
#         uploaded_files = []

#         # Ensure upload folder exists
#         db_folder = os.path.join(UPLOAD_ROOT, db_type)
#         os.makedirs(db_folder, exist_ok=True)
#         print(db_folder)
#         logging.debug(f"Upload folder ensured: {db_folder}")

#         # Handle MySQL uploads
#         if db_type.lower() == "mysql":
#             clear_existing_mysql_tables()  # Clear existing MySQL tables

#             for file in files:
#                 if file.filename == "":
#                     logging.warning("One or more files have no filename.")
#                     return jsonify({"error": "One or more files have no filename"}), 400
#                 file_path = os.path.join(db_folder, file.filename)
#                 file.save(file_path)
#                 uploaded_files.append(file_path)
#                 logging.info(f"File saved: {file_path}")

#                 # Process the file and insert into MySQL
#                 process_csv_to_mysql(file_path)

#         # Handle MongoDB uploads
#         elif db_type.lower() == "mongodb":
#             for file in files:
#                 if file.filename == "":
#                     logging.warning("One or more files have no filename.")
#                     return jsonify({"error": "One or more files have no filename"}), 400
#                 file_path = os.path.join(db_folder, file.filename)
#                 file.save(file_path)
#                 uploaded_files.append(file_path)
#                 logging.info(f"File saved: {file_path}")

#                 # Process the file and insert into MongoDB
#                 process_csv_to_mongodb(file_path)

#         else:
#             logging.warning(f"Unsupported database type: {db_type}")
#             return jsonify({"error": "Unsupported database type"}), 400

#         logging.info(f"All files processed successfully for {db_type}.")
#         return jsonify({
#             "message": f"{len(files)} files uploaded and processed successfully!",
#             "dbType": db_type,
#             "files": uploaded_files
#         }), 200

#     except Exception as e:
#         logging.error(f"Error during file upload: {e}")
#         return jsonify({"error": f"Server error: {str(e)}"}), 500

# if __name__ == "__main__":
#     initialize_mysql_database()  # Ensure MySQL database is initialized
#     app.run(port=3001)
