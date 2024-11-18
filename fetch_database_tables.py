from flask import Flask, request, jsonify
from flask_cors import CORS
import mysql.connector

app = Flask(__name__)
CORS(app)

# Default MySQL connection settings (without the database)
MYSQL_BASE_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "password123",
}

def get_mysql_connection(database_name):
    """Dynamically set the database name for the connection."""
    try:
        config = MYSQL_BASE_CONFIG.copy()
        config["database"] = database_name
        conn = mysql.connector.connect(**config)
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        return None

def fetch_tables_with_sample_data(database_name):
    """Fetch table names, columns, and top 5 rows of data."""
    try:
        conn = get_mysql_connection(database_name)
        if not conn:
            return {"error": f"Unable to connect to database: {database_name}"}

        cursor = conn.cursor(dictionary=True)

        # Get all tables in the database
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()

        if not tables:
            return []

        # Extract table names dynamically
        table_key = list(tables[0].keys())[0]  # Get the first key dynamically
        result = []
        for table in tables:
            table_name = table[table_key]

            # Fetch column names
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`;")
            columns = [col["Field"] for col in cursor.fetchall()]

            # Fetch top 5 rows
            cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 5;")
            sample_data = cursor.fetchall()

            result.append({
                "name": table_name,
                "columns": columns,
                "sampleData": sample_data
            })

        conn.close()
        return result

    except mysql.connector.Error as err:
        print(f"Error fetching data: {err}")
        return []

@app.route("/fetch-data", methods=["GET"])
def fetch_data():
    """API Endpoint to fetch table data."""
    db_type = request.args.get("dbType")  # Capture the database type
    db_name = request.args.get("dbName")  # Capture the database name
    print(db_name, db_type)

    if not db_name:
        return jsonify({"error": "Database name not provided"}), 400

    if db_type == "mysql":
        tables_data = fetch_tables_with_sample_data(db_name)
        if "error" in tables_data:
            return jsonify({"error": tables_data["error"]}), 500
        return jsonify({"tables": tables_data}), 200
    else:
        return jsonify({"error": "Unsupported database type"}), 400

if __name__ == "__main__":
    app.run(port=3001)
