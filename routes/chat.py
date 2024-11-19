from flask import Blueprint, request, jsonify
from services.query_generator import MySQLTemplate
import random

chat_bp = Blueprint("chat", __name__)

@chat_bp.route("/api/ask-question", methods=["POST"])
def ask_question():
    """Handle chat messages and respond with sample SQL queries."""
    data = request.get_json()

    user_message = data.get("question")
    db_name = data.get("dbName")
    db_type = data.get("dbType")

    if not user_message or not db_name or not db_type:
        return jsonify({"error": "Invalid request data"}), 400

    if db_type.lower() != "mysql":
        return jsonify({"error": f"Database type {db_type} is not supported"}), 400

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
        print(f"Error generating query: {e}")
        return jsonify({"error": f"Failed to generate query: {str(e)}"}), 500


def generate_random_queries(mysql_template, count=5):
    """Generate a list of random queries using the MySQLTemplate."""
    templates = [
        mysql_template.template_select,
        mysql_template.template_distinct,
        mysql_template.template_where,
        mysql_template.template_order_by,
        mysql_template.template_group_by,
        mysql_template.template_join,
        mysql_template.template_in,
        mysql_template.template_between,
        mysql_template.template_having,
    ]
    
    random_queries = []
    for _ in range(count):
        template_func = random.choice(templates)
        try:
            query = template_func()["query"]
            random_queries.append(query)
        except Exception as e:
            print(f"Error generating random query: {e}")
            continue

    return random_queries

@chat_bp.route("/api/run-query", methods=["POST"])
def run_query():
    """Execute the provided SQL query and return the result."""
    data = request.get_json()
    query = data.get("query")
    db_name = data.get("dbName")
    db_type = data.get("dbType")

    if not query or not db_name or not db_type:
        return jsonify({"error": "Invalid request data"}), 400

    if db_type.lower() != "mysql":
        return jsonify({"error": f"Database type {db_type} is not supported"}), 400

    try:
        # Initialize the MySQL connection for the specified database
        mysql_template = MySQLTemplate(db_name)

        # Execute the query with enforced limit
        result = mysql_template.execute_query(query)
        return jsonify({"result": result}), 200
    except Exception as e:
        print(f"Error executing query: {e}")
        return jsonify({"error": f"Failed to execute query: {str(e)}"}), 500