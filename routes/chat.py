from flask import Blueprint, request, jsonify
import mysql.connector
from pymongo import MongoClient

chat_bp = Blueprint("chat", __name__)

# MySQL Configuration
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "password123",
}

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/"
mongo_client = MongoClient(MONGO_URI)

def get_mysql_connection(db_name):
    """Dynamically connect to the MySQL database."""
    config = MYSQL_CONFIG.copy()
    config["database"] = db_name
    return mysql.connector.connect(**config)

def fetch_response_from_mysql(db_name, question):
    """Handle user questions for MySQL."""
    try:
        conn = get_mysql_connection(db_name)
        cursor = conn.cursor()

        # Example: Simulate processing the question
        if "tables" in question.lower():
            cursor.execute("SHOW TABLES;")
            tables = [row[0] for row in cursor.fetchall()]
            return f"The tables in the database are: {', '.join(tables)}."
        else:
            return "I'm sorry, I couldn't understand your question."

    except mysql.connector.Error as err:
        print(f"Error interacting with MySQL: {err}")
        return "An error occurred while processing your request."
    finally:
        if conn:
            conn.close()

def fetch_response_from_mongo(db_name, question):
    """Handle user questions for MongoDB."""
    try:
        db = mongo_client[db_name]

        # Example: Simulate processing the question
        if "collections" in question.lower():
            collections = db.list_collection_names()
            return f"The collections in the database are: {', '.join(collections)}."
        else:
            return "I'm sorry, I couldn't understand your question."

    except Exception as e:
        print(f"Error interacting with MongoDB: {e}")
        return "An error occurred while processing your request."

@chat_bp.route("/api/ask-question", methods=["POST"])
def ask_question():
    """API endpoint to handle user questions."""
    data = request.json
    question = data.get("question", "")
    db_name = data.get("dbName", "")
    db_type = data.get("dbType", "")

    if not question or not db_name or not db_type:
        return jsonify({"answer": "Invalid request. Please provide a question, database name, and database type."}), 400

    if db_type == "mysql":
        answer = fetch_response_from_mysql(db_name, question)
    elif db_type == "mongodb":
        answer = fetch_response_from_mongo(db_name, question)
    else:
        answer = "Unsupported database type."

    return jsonify({"answer": answer})
