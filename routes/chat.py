from flask import Blueprint, request, jsonify
from services.mysql_query_generator import MySQLTemplate
from services.mongodb_template import MongoDBTemplate  # Import MongoDB template
import random
import ast
import json
from bson.decimal128 import Decimal128
from bson import ObjectId
import re


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

            # Check if the user's input starts with "example sql" or "random queries"
            user_message_lower = user_message.lower()
            if user_message_lower.startswith("example sql") or user_message_lower.startswith("random queries"):
                random_queries = generate_random_queries(mysql_template, count=5)
                return jsonify({"queries": random_queries, "message": "success"}), 200

            # Check if the user's input starts with "example"
            elif "example" in user_message_lower or "sample" in user_message_lower:
                # Find a specific template based on the user's message
                selected_template = None
                for keyword, template_func in template_map.items():
                    if keyword in user_message_lower:
                        selected_template = template_func
                        break

                if not selected_template:
                    return jsonify({"error": "Query type not identified"}), 400

                response = selected_template()
                return jsonify({"query_str": response["query_str"], "desc": response["desc"], "message": "success"}), 200

            # Otherwise, call the natural_language function
            else:
                response = mysql_template.natural_lang_query(user_message)
                return jsonify({"query_str": response["query_str"], "desc": response["desc"], "message": "success"}), 200

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
                    "find projection": mongodb_template.template_find_with_projection,
                    "find": mongodb_template.template_find,
                    "starts with": mongodb_template.template_find_regex_2,
                    "ends with": mongodb_template.template_find_regex_2,
                    "regex": mongodb_template.template_find_regex,
                    "math operations": mongodb_template.template_find_math_operations,
                    "insert many": mongodb_template.template_insert_many,
                    "insert one": mongodb_template.template_insert_one,
                    "or condition": mongodb_template.template_find_or,
                    "and condition": mongodb_template.template_find_and,
                    "all": mongodb_template.template_find_array_all,
                    "nested": mongodb_template.template_find_nested_attribute
                    
            }

            user_message_lower = user_message.lower()

            # Check for vague inputs
            if "example queries" in user_message_lower or "random queries" in user_message_lower:
                random_queries = generate_random_queries(mongodb_template, count=5)
                return jsonify(random_queries), 200
            
            elif "example" in user_message_lower or "sample" in user_message_lower:
                # Find a specific template based on the user's message
                selected_template = None
                for keyword, template_func in template_map.items():
                    if keyword in user_message.lower():
                        selected_template = template_func
                        break
                
                response = selected_template()
                return jsonify(response), 200
            
            # Otherwise, call the natural_language function
            else:
                response = mongodb_template.natural_lang_query_2(user_message)
                return jsonify(response), 200

        except Exception as e:
            print(f"Error generating MongoDB query: {e}")
            return jsonify({"error": f"Failed to generate MongoDB query: {str(e)}"}), 500

    else:
        return jsonify({"error": f"Database type {db_type} is not supported"}), 400

# Mapping of user intents to MongoDB templates
mongodb_template_mapping = {
    "math operations": ["sum", "average", "greater than", "less than", "more than"],
    "find projection": ["find all", "list all", "fetch all", "find projection"],
    "find": ["find", "fetch", "list"],
    "regex": ["match", "regex", "pattern"],
    "insert many": ["insert multiple", "add many", "create many"],
    "insert one": ["insert", "add", "create"],
    "or condition": ["either", "or"],
    "and condition": ["and", "both"],
    "all": ["all elements", "all values"],
    "nested": ["nested attribute", "embedded document"],
}

def identify_template_and_extract(user_input, known_collections, known_fields):
    """
    Identify the MongoDB template, collection name, field, and value from user input.
    """
    # Identify the template based on user input
    identified_template = None

    for template, keywords in mongodb_template_mapping.items():
        for keyword in keywords:
            if keyword in user_input.lower():
                identified_template = template
                break
        if identified_template:
            break

    if not identified_template:
        return {"error": "Template not identified"}

    # Extract collection name using known collections
    collection_name = None
    for collection in known_collections:
        collection_pattern = rf"\b{collection}\b"
        if re.search(collection_pattern, user_input, re.IGNORECASE):
            collection_name = collection
            break

    # Extract field, operator, and value using known fields
    field = None
    operator = None
    value = None
    for known_field in known_fields:
        field_pattern = rf"\b{known_field}\b"  # Match the field name as a whole word
        if re.search(field_pattern, user_input, re.IGNORECASE):
            field = known_field  # Field is found

            # Look for math operations or textual conditions
            value_pattern = rf"{known_field}.*?(is|are|serving|of|with)?\s+(['\"]?[a-zA-Z0-9\s]+['\"]?)"
            match = re.search(value_pattern, user_input, re.IGNORECASE)
            if match:
                operator = match.group(1).strip().lower() if match.group(1) else None  # Extract the operator
                value = match.group(2).strip().strip('"').strip("'")  # Extract and clean value
            break

    # Map operator to MongoDB query operator (for math operations)
    mongo_operator_map = {
        "greater than": "$gt",
        "more than": "$gt",
        "less than": "$lt",
        "fewer than": "$lt",
        "is": "$eq",
        "are": "$eq",
    }
    mongo_operator = mongo_operator_map.get(operator, "$eq")  # Default to "$eq" if no operator

    # Prepare query condition for math operations
    query_condition = {field: {mongo_operator: value}} if field and value else {}

    return {
        "template": identified_template,
        "collection": collection_name,
        "field": field,
        "value": value,
        "operator": operator,
        "mongo_operator": mongo_operator,
        "query_condition": query_condition,
    }


def generate_random_queries(template, count=5):
    """Generate a list of random queries using the template."""
    if isinstance(template, MySQLTemplate):
        templates = [
            template.template_select,
            template.template_distinct,
            template.template_where,
            template.template_order_by,
            template.template_group_by,
            template.template_in,
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
            # query = template_func()["query"] if isinstance(template, MySQLTemplate) else template_func()
            query = template_func()
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