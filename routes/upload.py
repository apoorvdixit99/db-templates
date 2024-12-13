from flask import Blueprint, request, jsonify
from services.mysql_service import get_mysql_connection
from pymongo import MongoClient
import pandas as pd
from config.mongo_config import MONGO_URI

upload_bp = Blueprint("upload", __name__)

@upload_bp.route("/upload", methods=["POST"])
def upload_files():
    db_type = request.form.get("dbType")
    files = request.files.getlist("files")

    if not db_type or not files:
        return jsonify({"error": "Invalid request"}), 400

    if db_type == "mysql":
        try:
            conn = get_mysql_connection("userdatabase")
            cursor = conn.cursor()

            for file in files:
                # Process CSV file
                df = pd.read_csv(file)
                table_name = file.filename.split(".")[0]
                columns = ", ".join([f"`{col}` TEXT" for col in df.columns])
                cursor.execute(f"CREATE TABLE IF NOT EXISTS `{table_name}` ({columns});")

                for _, row in df.iterrows():
                    placeholders = ", ".join(["%s"] * len(row))
                    cursor.execute(f"INSERT INTO `{table_name}` VALUES ({placeholders});", tuple(row))

                conn.commit()

            conn.close()
            return jsonify({"message": "Files uploaded and processed successfully to MySQL!"}), 200

        except Exception as e:
            return jsonify({"error": str(e)}), 500

    elif db_type == "mongodb":
        try:
            client = MongoClient(MONGO_URI)
            db = client["userdatabase"]

            # Drop all existing collections in the database
            collections = db.list_collection_names()
            for collection_name in collections:
                db[collection_name].drop()
                print(f"MongoDB Collection '{collection_name}' dropped.")

            for file in files:
                # Process CSV file
                df = pd.read_csv(file)
                collection_name = file.filename.split(".")[0]

                # Insert data into MongoDB
                data = df.to_dict(orient="records")  # Convert DataFrame to list of dicts
                collection = db[collection_name]
                collection.insert_many(data)
                print(f"Data inserted into MongoDB Collection '{collection_name}'.")

            client.close()
            return jsonify({"message": "Files uploaded and processed successfully to MongoDB!"}), 200

        except Exception as e:
            return jsonify({"error": str(e)}), 500

    else:
        return jsonify({"error": "Unsupported database type"}), 400
