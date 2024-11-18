import mysql.connector
from config.mysql_config import MYSQL_CONFIG

def get_mysql_connection(database_name):
    """Get a MySQL connection for the specified database."""
    config = MYSQL_CONFIG.copy()
    config["database"] = database_name
    return mysql.connector.connect(**config)

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

        result.append({"name": table_name, "columns": columns, "sampleData": sample_data})

    conn.close()
    return result