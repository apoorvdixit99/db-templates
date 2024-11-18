from flask import Blueprint, request, jsonify
from services.mysql_service import get_mysql_connection
import pandas as pd

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
            return jsonify({"message": "Files uploaded and processed successfully!"}), 200

        except Exception as e:
            return jsonify({"error": str(e)}), 500

    elif db_type == "mongodb":
        return jsonify({"error": "MongoDB upload not implemented yet"}), 400

    else:
        return jsonify({"error": "Unsupported database type"}), 400