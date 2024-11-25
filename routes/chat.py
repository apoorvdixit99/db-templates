from flask import Blueprint, request, jsonify
from services.mysql_query_generator import MySQLTemplate
from templates.mongodb_template import MongoDBTemplate  # Import MongoDB template
import random
import ast
import json
from bson.decimal128 import Decimal128
from bson import ObjectId


chat_bp = Blueprint("chat", __name__)

@chat_bp.route("/api/ask-question", methods=["POST"])
def ask_question():
    """Handle chat messages and respond with sample queries for MySQL or MongoDB."""
    data = request.get_json()

    user_message = data.get("question")
    db_name = data.get("dbName")
    db_type = data.get("dbType")

    if not user_message or not db_name or not db_type:
        return jsonify({"error": "Invalid request data"}), 400

    if db_type.lower() == "mysql":
        try:
            mysql_template = MySQLTemplate(db_name)  # Initialize with the provided database name

            # Example Mapping for Specific Queries
            template_map = {
                "select": mysql_template.template_select,
                "distinct": mysql_template.template_distinct,
                "where": mysql_template.template_where,
                "order by": mysql_template.template_order_by,
                "group by": mysql_template.template_group_by,
                "join": mysql_template.template_join,
                "in": mysql_template.template_in,
                "between": mysql_template.template_between,
                "having": mysql_template.template_having,
            }

            # Check for vague inputs
            if "example sql" in user_message.lower() or "random queries" in user_message.lower():
                random_queries = generate_random_queries(mysql_template, count=5)
                return jsonify({"queries": random_queries, "message": "success"}), 200

            # Find a specific template based on the user's message
            selected_template = None
            for keyword, template_func in template_map.items():
                if keyword in user_message.lower():
                    selected_template = template_func
                    break

            if not selected_template:
                return jsonify({"error": "Query type not identified"}), 400

            response = selected_template()
            return jsonify({"query": response["query"], "message": "success"}), 200

        except Exception as e:
            print(f"Error generating MySQL query: {e}")
            return jsonify({"error": f"Failed to generate MySQL query: {str(e)}"}), 500

    elif db_type.lower() == "mongodb":
        try:
            if db_name == "Weather":
                db_name = "sample_weatherdata"
            elif db_name != "userdatabase":
                db_name = db_name if db_name == "userdatabase" else "sample_" + db_name.lower()
        
            # Debugging print to verify db_name
            print(f"Constructed database name: {db_name}")
            mongodb_template = MongoDBTemplate(db_name)  # Initialize with the provided database name

            # Example Mapping for Specific Queries
            template_map = {
                    "find": mongodb_template.template_find,
                    # "find and sort": mongodb_template.template_find_and_sort,
                    # "in": mongodb_template.template_find_array_in,
                    "regex": mongodb_template.template_find_regex,
                    # "regex": mongodb_template.template_find_regex,
                    "math operations": mongodb_template.template_find_math_operations,
                    "insert many": mongodb_template.template_insert_many,
                    "insert one": mongodb_template.template_insert_one,
                    # "update one": mongodb_template.template_update_one,
                    # "delete many": mongodb_template.template_delete_many,
                    "or condition": mongodb_template.template_find_or,
                    "and condition": mongodb_template.template_find_and,
                    "all": mongodb_template.template_find_array_all,
                    "nested": mongodb_template.template_find_nested_attribute
            }


            # Check for vague inputs
            if "example queries" in user_message.lower() or "random queries" in user_message.lower():
                random_queries = generate_random_queries(mongodb_template, count=5)
                print(random_queries)
                # return jsonify({"queries": random_queries, "message": "success"}), 200
                return jsonify(random_queries), 200

            # Find a specific template based on the user's message
            selected_template = None
            for keyword, template_func in template_map.items():
                if keyword in user_message.lower():
                    selected_template = template_func
                    print(keyword)
                    break

            if not selected_template:
                return jsonify({"error": "Query type not identified"}), 400

            response = selected_template()
            print(response)
            return jsonify(response), 200

        except Exception as e:
            print(f"Error generating MongoDB query: {e}")
            return jsonify({"error": f"Failed to generate MongoDB query: {str(e)}"}), 500

    else:
        return jsonify({"error": f"Database type {db_type} is not supported"}), 400


def generate_random_queries(template, count=5):
    """Generate a list of random queries using the template."""
    if isinstance(template, MySQLTemplate):
        templates = [
            template.template_select,
            template.template_distinct,
            template.template_where,
            template.template_order_by,
            template.template_group_by,
            template.template_join,
            template.template_in,
            template.template_between,
            template.template_having,
        ]
    elif isinstance(template, MongoDBTemplate):
        templates = [
            template.template_find,
            template.template_find_array_in,
            template.template_find_regex,
            template.template_find_math_operations,
            template.template_insert_many,
            template.template_insert_one,
            template.template_find_or,
            template.template_find_and,
            template.template_find_array_all,
            template.template_find_nested_attribute
        ]
    else:
        return []

    random_queries = []
    for _ in range(count):
        template_func = random.choice(templates)
        try:
            query = template_func()["query"] if isinstance(template, MySQLTemplate) else template_func()
            random_queries.append(query)
        except Exception as e:
            print(f"Error generating random query: {e}")
            continue

    return random_queries

def make_json_serializable(data):
    """Recursively convert MongoDB data to JSON-serializable format."""
    if isinstance(data, Decimal128):
        return float(data.to_decimal())  # Convert Decimal128 to float
    elif isinstance(data, ObjectId):
        return str(data)  # Convert ObjectId to string
    elif isinstance(data, dict):
        return {key: make_json_serializable(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [make_json_serializable(item) for item in data]
    else:
        return data

@chat_bp.route("/api/run-query", methods=["POST"])
def run_query():
    """Execute the provided query for MySQL or MongoDB and return the result."""
    data = request.get_json()

    query = data.get("query")
    db_name = data.get("dbName")
    db_type = data.get("dbType")

    if not query or not db_name or not db_type:
        return jsonify({"error": "Invalid request data. Please provide 'query', 'dbName', and 'dbType'"}), 400

    try:
        if db_type.lower() == "mysql":
            # Handle MySQL queries
            template = MySQLTemplate(db_name)
            result = template.execute_query(query)
            return jsonify({"result": result}), 200

        elif db_type.lower() == "mongodb":
            # Handle MongoDB queries
            try:
                # Normalize database names
                if db_name == "Weather":
                    db_name = "sample_weatherdata"
                elif db_name != "userdatabase":
                    db_name = db_name if db_name == "userdatabase" else "sample_" + db_name.lower()

                print(f"Constructed database name: {db_name}")  # Debugging print
                template = MongoDBTemplate(db_name)
                print(query)
                
                # Execute the MongoDB query directly
                result = template.execute_query(query)

                # Serialize the result to make it JSON-compatible
                serializable_result = make_json_serializable(result)

                # Handle specific query types
                if query.get("query_type") in ["insertOne", "insertMany"]:
                    return jsonify({"result": str(serializable_result)}), 200
                else:
                    return jsonify({"result": serializable_result}), 200

            except Exception as e:
                print(f"Error executing MongoDB query: {e}")  # Log the error
                return jsonify({"error": f"Failed to execute MongoDB query: {str(e)}"}), 500

        else:
            return jsonify({"error": f"Unsupported database type: {db_type}"}), 400

    except Exception as e:
        print(f"Unexpected error executing query: {e}")  # Log the error
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500