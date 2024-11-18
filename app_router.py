from flask import Flask, request, jsonify
from app_mysql import app as mysql_app  # Import the existing MySQL app

app = Flask(__name__)

@app.route('/query', methods=['POST'])
def handle_query():
    """
    Handle dynamic queries for different database types.
    Expected request body:
    {
        "db_type": "mysql",
        "query_type": "select",  # or "limit", etc.
        "params": { ... }       # Parameters required for the query
    }
    """
    try:
        # Parse the request body
        data = request.json
        db_type = data.get("db_type")
        query_type = data.get("query_type")
        params = data.get("params", {})

        # Validate required fields
        if not db_type or not query_type:
            return jsonify({"error": "Missing db_type or query_type"}), 400

        if db_type.lower() == "mysql":
            # Delegate query to MySQL app based on query_type
            if query_type == "select":
                limit = params.get("limit", 5)  # Default to 5 if not provided
                response = mysql_app.test_client().get(f"/select/{limit}")
                return jsonify(response.json)


            elif query_type == "limit":
                response = mysql_app.test_client().get("/limit")
                return jsonify(response.json)

            else:
                return jsonify({"error": f"Unsupported query_type: {query_type}"}), 400

        else:
            return jsonify({"error": f"Unsupported db_type: {db_type}"}), 400

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=8080)
