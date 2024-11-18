from flask import Flask, jsonify, request
import mysql.connector
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

app = Flask(__name__)
port = int(os.getenv('PORT', 5000))

# MySQL Setup
mysql_connection = mysql.connector.connect(
    host=os.getenv('HOST'),
    user=os.getenv('USER'),
    password=os.getenv('PASSWORD'),
    database=os.getenv('DATABASE')
)
mysql_cursor = mysql_connection.cursor()

# MongoDB Setup
mongo_url = os.getenv('MONGO_URL')
mongo_client = MongoClient(mongo_url)
mongo_db = mongo_client['yourMongoDBDatabase']

@app.route('/', methods=['GET'])
def index():
    return "API is working!"

# MySQL: Fetch Table Names
@app.route('/mysql/tables', methods=['GET'])
def get_mysql_tables():
    try:
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = %s"
        mysql_cursor.execute(query, (os.getenv('DATABASE'),))
        results = mysql_cursor.fetchall()
        tables = [row[0] for row in results]
        return jsonify(tables)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# MySQL: Fetch Schema of a Table
@app.route('/mysql/schema/<string:table>', methods=['GET'])
def get_mysql_schema(table):
    try:
        query = "SHOW COLUMNS FROM {}".format(table)
        mysql_cursor.execute(query)
        results = mysql_cursor.fetchall()
        columns = [{"Field": row[0], "Type": row[1], "Null": row[2], "Key": row[3], "Default": row[4], "Extra": row[5]} for row in results]
        return jsonify(columns)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# MongoDB: Fetch Collection Names
@app.route('/mongodb/collections', methods=['GET'])
def get_mongodb_collections():
    try:
        collections = mongo_db.list_collection_names()
        return jsonify(collections)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# MongoDB: Fetch Data from a Collection
@app.route('/mongodb/<string:collection>', methods=['GET'])
def get_mongodb_collection_data(collection):
    try:
        mongo_collection = mongo_db[collection]
        documents = list(mongo_collection.find({}))
        # Convert ObjectId to string for JSON serialization
        for doc in documents:
            doc['_id'] = str(doc['_id'])
        return jsonify(documents)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Error Handling
@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Something broke!"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=True)
