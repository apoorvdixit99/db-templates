import mysql.connector
from config.mysql_config import MYSQL_CONFIG
from datetime import datetime
from decimal import Decimal

def get_mysql_connection(database_name):
    """Get a MySQL connection for the specified database."""
    config = MYSQL_CONFIG.copy()
    config["database"] = database_name
    return mysql.connector.connect(**config)

def serialize_value(value):
    """Convert MySQL values to JSON-serializable formats."""
    if isinstance(value, bytes):
        return value.decode('utf-8', errors='ignore')  # Decode bytes to string
    elif isinstance(value, datetime):
        return value.isoformat()  # Convert datetime to ISO 8601 string
    elif isinstance(value, Decimal):
        return float(value)  # Convert Decimal to float
    elif isinstance(value, set):
        return list(value)  # Convert set to list
    return value  # Return the value as is for other types

def fetch_mysql_tables_with_sample_data(database_name):
    """Fetch tables and sample data from MySQL."""
    conn = get_mysql_connection(database_name)
    cursor = conn.cursor(dictionary=True)

    # Fetch all tables
    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()

    result = []
    for table in tables:
        table_name = list(table.values())[0]

        # Fetch columns
        cursor.execute(f"SHOW COLUMNS FROM `{table_name}`;")
        columns = [col["Field"] for col in cursor.fetchall()]

        # Fetch sample data
        cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 5;")
        sample_data = cursor.fetchall()

        # Serialize the sample data to ensure it's JSON-serializable
        serialized_data = [
            {key: serialize_value(value) for key, value in row.items()}
            for row in sample_data
        ]

        result.append({"name": table_name, "columns": columns, "sampleData": serialized_data})

    conn.close()
    return result
