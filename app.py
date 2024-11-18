from flask import Flask
from flask_cors import CORS
from routes.fetch_data import fetch_data_bp
from routes.upload import upload_bp
from routes.chat import chat_bp

app = Flask(__name__)
CORS(app)

# Register Blueprints
app.register_blueprint(fetch_data_bp)
app.register_blueprint(upload_bp)
app.register_blueprint(chat_bp)

if __name__ == "__main__":
    app.run(debug=True, port=3001)


# from flask import Flask, request, jsonify, g
# import mysql.connector
# from mysql_template import MySQLTemplate
# from pymongo import MongoClient

# app = Flask(__name__)

# # Initialize MySQLTemplate
# mysql_template = MySQLTemplate()

# # MySQL Configuration
# MYSQL_CONFIG = {
#     "host": "localhost",
#     "user": "root",
#     "password": "root123",
# }

# # MongoDB Configuration
# MONGO_URI = "mongodb://localhost:27017/"
# mongo_client = MongoClient(MONGO_URI)

# def get_mysql_connection(db_name):
#     """Get a MySQL connection for the specified database."""
#     connection = mysql.connector.connect(database=db_name, **MYSQL_CONFIG)
#     return connection

# def get_mongo_db(db_name):
#     """Get a MongoDB database instance."""
#     return mongo_client[db_name]

# @app.teardown_appcontext
# def close_connection(exception):
#     """Clean up database connections."""
#     if hasattr(g, "mysql_connection"):
#         g.mysql_connection.close()
#     if hasattr(g, "mongo_client"):
#         mongo_client.close()

# if __name__ == '__main__':
#     app.run(debug=True, host="127.0.0.1", port=5050)


# @app.route('/query', methods=['POST'])
# def query_data():
#     """
#     API endpoint for querying data from MySQL or MongoDB.
#     Expected JSON payload:
#     {
#         "db_type": "mysql" or "mongodb",
#         "db_name": "<database_name>",
#         "query": "<SQL query or MongoDB operation>"
#     }
#     """
#     data = request.json
#     db_type = data.get("db_type")
#     db_name = data.get("db_name")
#     query = data.get("query")
    
#     if not db_type or not db_name or not query:
#         return jsonify({"error": "Missing required parameters: db_type, db_name, query"}), 400

#     try:
#         if db_type.lower() == "mysql":
#             # Execute MySQL query
#             connection = get_mysql_connection(db_name)
#             cursor = connection.cursor()
#             cursor.execute(query)
#             result = cursor.fetchall()
#             connection.close()
#             return jsonify({"db_type": "mysql", "db_name": db_name, "result": result})

#         elif db_type.lower() == "mongodb":
#             # Execute MongoDB operation
#             db = get_mongo_db(db_name)
#             result = eval(f"db.{query}")
#             if isinstance(result, list):
#                 result = [doc for doc in result]
#             return jsonify({"db_type": "mongodb", "db_name": db_name, "result": result})

#         else:
#             return jsonify({"error": "Invalid db_type. Use 'mysql' or 'mongodb'"}), 400

#     except Exception as e:
#         return jsonify({"error": str(e)}), 500

# @app.route('/databases', methods=['GET'])
# def list_databases():
#     """
#     API endpoint to list all available databases in MySQL and MongoDB.
#     """
#     try:
#         # List MySQL databases
#         mysql_connection = mysql.connector.connect(**MYSQL_CONFIG)
#         mysql_cursor = mysql_connection.cursor()
#         mysql_cursor.execute("SHOW DATABASES")
#         mysql_databases = [db[0] for db in mysql_cursor.fetchall()]
#         mysql_connection.close()

#         # List MongoDB databases
#         mongo_databases = mongo_client.list_database_names()

#         return jsonify({
#             "mysql_databases": mysql_databases,
#             "mongodb_databases": mongo_databases
#         })

#     except Exception as e:
#         return jsonify({"error": str(e)}), 500

