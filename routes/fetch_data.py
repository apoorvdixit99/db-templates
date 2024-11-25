from flask import Blueprint, request, jsonify
from services.mysql_service import fetch_mysql_tables_with_sample_data
from services.mongo_service import fetch_mongo_collections_with_sample_data, decimal128_to_json_serializable

fetch_data_bp = Blueprint("fetch_data", __name__)

@fetch_data_bp.route("/fetch-data", methods=["GET"])
def fetch_data():
    db_type = request.args.get("dbType")
    db_name = request.args.get("dbName")

    if not db_name:
        return jsonify({"error": "Database name not provided"}), 400

    if db_type == "mysql":
        try:
            data = fetch_mysql_tables_with_sample_data(db_name)
            return jsonify({"tables": data}), 200
        except Exception as e:
            print(str(e))
            return jsonify({"error": str(e)}), 500

    elif db_type == "mongodb":
        try:
            if db_name == "Weather":
                db_name = "sample_weatherdata"
            else:
                db_name = db_name if db_name == "userdatabase" else "sample_" + db_name.lower()
        
            # Debugging print to verify db_name
            print(f"Constructed database name: {db_name}")
            data = fetch_mongo_collections_with_sample_data(db_name)
            
            # Convert any Decimal128 values to JSON-serializable format
            serializable_data = decimal128_to_json_serializable(data)
            
            return jsonify({"collections": serializable_data}), 200
        except Exception as e:
            print(str(e))
            return jsonify({"error": str(e)}), 500

    else:
        return jsonify({"error": "Unsupported database type"}), 400