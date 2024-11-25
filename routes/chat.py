# from flask import Blueprint, request, jsonify
# from services.mysql_query_generator import MySQLTemplate
# from services.mongodb_query_generator import MongoDBTemplate  # Import MongoDB template
# import random
# import ast
# import json
# from bson.decimal128 import Decimal128
# from bson import ObjectId


# chat_bp = Blueprint("chat", __name__)

# @chat_bp.route("/api/ask-question", methods=["POST"])
# def ask_question():
#     """Handle chat messages and respond with sample queries for MySQL or MongoDB."""
#     data = request.get_json()

#     user_message = data.get("question")
#     db_name = data.get("dbName")
#     db_type = data.get("dbType")

#     if not user_message or not db_name or not db_type:
#         return jsonify({"error": "Invalid request data"}), 400

#     if db_type.lower() == "mysql":
#         try:
#             mysql_template = MySQLTemplate(db_name)  # Initialize with the provided database name

#             # Example Mapping for Specific Queries
#             template_map = {
#                 "select": mysql_template.template_select,
#                 "distinct": mysql_template.template_distinct,
#                 "where": mysql_template.template_where,
#                 "order by": mysql_template.template_order_by,
#                 "group by": mysql_template.template_group_by,
#                 "join": mysql_template.template_join,
#                 "in": mysql_template.template_in,
#                 "between": mysql_template.template_between,
#                 "having": mysql_template.template_having,
#             }

#             # Check for vague inputs
#             if "example sql" in user_message.lower() or "random queries" in user_message.lower():
#                 random_queries = generate_random_queries(mysql_template, count=5)
#                 return jsonify({"queries": random_queries, "message": "success"}), 200

#             # Find a specific template based on the user's message
#             selected_template = None
#             for keyword, template_func in template_map.items():
#                 if keyword in user_message.lower():
#                     selected_template = template_func
#                     break

#             if not selected_template:
#                 return jsonify({"error": "Query type not identified"}), 400

#             response = selected_template()
#             return jsonify({"query": response["query"], "message": "success"}), 200

#         except Exception as e:
#             print(f"Error generating MySQL query: {e}")
#             return jsonify({"error": f"Failed to generate MySQL query: {str(e)}"}), 500

#     elif db_type.lower() == "mongodb":
#         try:
#             if db_name == "Weather":
#                 db_name = "sample_weatherdata"
#             elif db_name != "userdatabase":
#                 db_name = db_name if db_name == "userdatabase" else "sample_" + db_name.lower()
        
#             # Debugging print to verify db_name
#             print(f"Constructed database name: {db_name}")
#             mongodb_template = MongoDBTemplate(db_name)  # Initialize with the provided database name

#             # Example Mapping for Specific Queries
#             template_map = {
#                 "find": mongodb_template.template_find,
#                 "find and sort": mongodb_template.template_find_and_sort,
#                 "count": mongodb_template.template_count_documents,
#                 "regex": mongodb_template.template_regex,
#                 "math operations": mongodb_template.template_math_operations,
#                 "update one": mongodb_template.template_update_one,
#                 "delete many": mongodb_template.template_delete_many,
#                 "or condition": mongodb_template.template_find_or,
#             }


#             # Check for vague inputs
#             if "example queries" in user_message.lower() or "random queries" in user_message.lower():
#                 random_queries = generate_random_queries(mongodb_template, count=5)
#                 print(random_queries)
#                 return jsonify({"queries": random_queries, "message": "success"}), 200

#             # Find a specific template based on the user's message
#             selected_template = None
#             for keyword, template_func in template_map.items():
#                 if keyword in user_message.lower():
#                     selected_template = template_func
#                     break

#             if not selected_template:
#                 return jsonify({"error": "Query type not identified"}), 400

#             response = selected_template()
#             print(response)
#             return jsonify({"query": response["query_str"], "message": "success"}), 200

#         except Exception as e:
#             print(f"Error generating MongoDB query: {e}")
#             return jsonify({"error": f"Failed to generate MongoDB query: {str(e)}"}), 500

#     else:
#         return jsonify({"error": f"Database type {db_type} is not supported"}), 400


# def generate_random_queries(template, count=5):
#     """Generate a list of random queries using the template."""
#     if isinstance(template, MySQLTemplate):
#         templates = [
#             template.template_select,
#             template.template_distinct,
#             template.template_where,
#             template.template_order_by,
#             template.template_group_by,
#             template.template_join,
#             template.template_in,
#             template.template_between,
#             template.template_having,
#         ]
#     elif isinstance(template, MongoDBTemplate):
#         templates = [
#             template.template_insert_one,
#             template.template_insert_many,
#             template.template_find,
#             template.template_regex,
#             template.template_math_operations,
#         ]
#     else:
#         return []

#     random_queries = []
#     for _ in range(count):
#         template_func = random.choice(templates)
#         try:
#             query = template_func()["query"] if isinstance(template, MySQLTemplate) else template_func()["query_str"]
#             random_queries.append(query)
#         except Exception as e:
#             print(f"Error generating random query: {e}")
#             continue

#     return random_queries

# def parse_raw_mongo_query(raw_query):
#     """
#     Parse raw MongoDB shell-style query into the structure expected by execute_query.
#     """
#     try:
#         if not raw_query.startswith("db."):
#             raise ValueError("Query does not start with 'db.'")

#         # Extract collection and operation
#         parts = raw_query.split(".", 2)
#         if len(parts) < 2:
#             raise ValueError("Invalid MongoDB query format")

#         collection = parts[1].split("(")[0].strip()  # Extract collection name

#         # Detect the query type
#         if "find(" in raw_query:
#             params = raw_query.split("find(", 1)[1].rstrip(")")
#             query_params, projection = "{}", "{}"  # Defaults

#             if "," in params:
#                 # Split into query parameters and projection
#                 query_params, projection = map(str.strip, params.split(",", 1))
#             else:
#                 query_params = params.strip()

#             query_params = ast.literal_eval(query_params) if query_params else {}
#             projection = ast.literal_eval(projection) if projection else {}

#             return {
#                 "query_type": "find",
#                 "collection": collection,
#                 "query_params": query_params,
#                 "query_projection": projection,
#             }

#         elif "insertOne(" in raw_query:
#             params = raw_query.split("insertOne(", 1)[1].rstrip(")")
#             document = ast.literal_eval(params)
#             return {
#                 "query_type": "insertOne",
#                 "collection": collection,
#                 "query_params": document,
#             }

#         elif "insertMany(" in raw_query:
#             params = raw_query.split("insertMany(", 1)[1].rstrip(")")
#             documents = ast.literal_eval(params)
#             return {
#                 "query_type": "insertMany",
#                 "collection": collection,
#                 "query_params": documents,
#             }

#         elif "updateOne(" in raw_query:
#             params = raw_query.split("updateOne(", 1)[1].rstrip(")")
#             filter_query, update_query = map(str.strip, params.split(",", 1))
#             filter_query = ast.literal_eval(filter_query)
#             update_query = ast.literal_eval(update_query)

#             return {
#                 "query_type": "updateOne",
#                 "collection": collection,
#                 "query_params": filter_query,
#                 "update_params": update_query,
#             }

#         elif "deleteMany(" in raw_query:
#             params = raw_query.split("deleteMany(", 1)[1].rstrip(")")
#             query = ast.literal_eval(params)

#             return {
#                 "query_type": "deleteMany",
#                 "collection": collection,
#                 "query_params": query,
#             }

#         else:
#             raise ValueError("Unsupported query type")

#     except Exception as e:
#         raise ValueError(f"Failed to parse MongoDB query: {str(e)}")

# def make_json_serializable(data):
#     """Recursively convert MongoDB data to JSON-serializable format."""
#     if isinstance(data, Decimal128):
#         return float(data.to_decimal())  # Convert Decimal128 to float
#     elif isinstance(data, ObjectId):
#         return str(data)  # Convert ObjectId to string
#     elif isinstance(data, dict):
#         return {key: make_json_serializable(value) for key, value in data.items()}
#     elif isinstance(data, list):
#         return [make_json_serializable(item) for item in data]
#     else:
#         return data

# @chat_bp.route("/api/run-query", methods=["POST"])
# def run_query():
#     """Execute the provided query for MySQL or MongoDB and return the result."""
#     data = request.get_json()

#     query = data.get("query")
#     db_name = data.get("dbName")
#     db_type = data.get("dbType")

#     if not query or not db_name or not db_type:
#         return jsonify({"error": "Invalid request data. Please provide 'query', 'dbName', and 'dbType'"}), 400

#     try:
#         if db_type.lower() == "mysql":
#             template = MySQLTemplate(db_name)
#             result = template.execute_query(query)
#             return jsonify({"result": result}), 200

#         elif db_type.lower() == "mongodb":
#             try:
#                 if db_name == "Weather":
#                     db_name = "sample_weatherdata"
#                 elif db_name != "userdatabase":
#                     db_name = db_name if db_name == "userdatabase" else "sample_" + db_name.lower()
            
#                 # Debugging print to verify db_name
#                 print(f"Constructed database name: {db_name}")
#                 template = MongoDBTemplate(db_name)
#                 transformed_query = parse_raw_mongo_query(query)
#                 result = template.execute_query(transformed_query)

#                 # Convert MongoDB result to JSON-serializable format
#                 serializable_result = make_json_serializable(result)

#                 # Handle specific query type responses
#                 if transformed_query["query_type"] in ["insertOne", "insertMany"]:
#                     return jsonify({"result": str(serializable_result)}), 200
#                 else:
#                     return jsonify({"result": serializable_result}), 200

#             except Exception as e:
#                 print(f"Error executing MongoDB query: {e}")
#                 return jsonify({"error": f"Failed to execute MongoDB query: {str(e)}"}), 500

#         else:
#             return jsonify({"error": f"Unsupported database type: {db_type}"}), 400

#     except Exception as e:
#         print(f"Unexpected error executing query: {e}")
#         return jsonify({"error": f"Unexpected error: {str(e)}"}), 500

from flask import Blueprint, request, jsonify
from services.mysql_query_generator import MySQLTemplate
from services.mongodb_query_generator import MongoDBTemplate
import random
import ast
import json
from bson.decimal128 import Decimal128
from bson import ObjectId
import re
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

# Ensure NLTK data is downloaded
nltk.download('punkt')
nltk.download('stopwords')

chat_bp = Blueprint("chat", __name__)

@chat_bp.route("/api/ask-question", methods=["POST"])
def ask_question():
    """Handle chat messages and respond with generated queries based on natural language input."""
    data = request.get_json()

    user_message = data.get("question")
    db_name = data.get("dbName")
    db_type = data.get("dbType")

    if not user_message or not db_name or not db_type:
        return jsonify({"error": "Invalid request data"}), 400

    # Initialize the appropriate template based on the database type
    if db_type.lower() == "mysql":
        try:
            template_instance = MySQLTemplate(db_name)
        except Exception as e:
            print(f"Error initializing MySQLTemplate: {e}")
            return jsonify({"error": f"Failed to initialize MySQLTemplate: {str(e)}"}), 500
    elif db_type.lower() == "mongodb":
        try:
            if db_name == "Weather":
                db_name = "sample_weatherdata"
            elif db_name != "userdatabase":
                db_name = db_name if db_name == "userdatabase" else "sample_" + db_name.lower()
            template_instance = MongoDBTemplate(db_name)
        except Exception as e:
            print(f"Error initializing MongoDBTemplate: {e}")
            return jsonify({"error": f"Failed to initialize MongoDBTemplate: {str(e)}"}), 500
    else:
        return jsonify({"error": f"Database type {db_type} is not supported"}), 400

    # Parse the user's question
    parsed_question = parse_question(user_message, db_type)
    if parsed_question:
        # Generate the query based on the parsed question
        query_info = generate_query(parsed_question, db_type, template_instance)
        if query_info:
            return jsonify({
                "query": query_info["query"],
                "natural_language": query_info["natural_language"],
                "message": "success"
            }), 200
        else:
            return jsonify({"error": "Could not generate query based on the question"}), 400
    else:
        # Existing logic for handling specific keywords and vague inputs
        if db_type.lower() == "mysql":
            try:
                mysql_template = template_instance  # Already initialized

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
                    return jsonify({"error": "Query pattern not recognized"}), 400

                response = selected_template()
                return jsonify({"query": response["query"], "natural_language": response.get("natural_language", ""), "message": "success"}), 200

            except Exception as e:
                print(f"Error generating MySQL query: {e}")
                return jsonify({"error": f"Failed to generate MySQL query: {str(e)}"}), 500

        elif db_type.lower() == "mongodb":
            try:
                mongodb_template = template_instance  # Already initialized

                # Example Mapping for Specific Queries
                template_map = {
                    "find": mongodb_template.template_find,
                    "find and sort": mongodb_template.template_find_and_sort,
                    "count": mongodb_template.template_count_documents,
                    "regex": mongodb_template.template_regex,
                    "math operations": mongodb_template.template_math_operations,
                    "update one": mongodb_template.template_update_one,
                    "delete many": mongodb_template.template_delete_many,
                    "or condition": mongodb_template.template_find_or,
                }

                # Check for vague inputs
                if "example queries" in user_message.lower() or "random queries" in user_message.lower():
                    random_queries = generate_random_queries(mongodb_template, count=5)
                    return jsonify({"queries": random_queries, "message": "success"}), 200

                # Find a specific template based on the user's message
                selected_template = None
                for keyword, template_func in template_map.items():
                    if keyword in user_message.lower():
                        selected_template = template_func
                        break

                if not selected_template:
                    return jsonify({"error": "Could not generate query based on the question"}), 400

                response = selected_template()
                return jsonify({"query": response["query_str"], "natural_language": response.get("natural_language", ""), "message": "success"}), 200

            except Exception as e:
                print(f"Error generating MongoDB query: {e}")
                return jsonify({"error": f"Failed to generate MongoDB query: {str(e)}"}), 500

        else:
            return jsonify({"error": f"Database type {db_type} is not supported"}), 400

def parse_question(question_text, db_type):
    """
    Parse the user's natural language question and extract variables based on predefined patterns.
    """
    question_text = question_text.lower()
    patterns = []

    # Define patterns for MySQL
    if db_type.lower() == "mysql":
        patterns = [
            {
                # Matches: "Customers living in country 'Germany'"
                "pattern": r"(.+?) living in (?:the )?(.+?)\s*'(.+?)'",
                "pattern_name": "select_where",
                "variables": ["Entity", "Field", "Value"]
            },
            {
                # Matches: "products having price greater than 10"
                "pattern": r"(.+?) having (.+?) (greater than|less than|=|>|<|>=|<=|!=|not equal to) (.+)",
                "pattern_name": "select_where_operator",
                "variables": ["Entity", "Field", "Operator", "Value"]
            },
            {
                # Matches: "products with price greater than 10"
                "pattern": r"(.+?) with (.+?) (greater than|less than|=|>|<|>=|<=|!=|not equal to) (.+)",
                "pattern_name": "select_where_operator",
                "variables": ["Entity", "Field", "Operator", "Value"]
            },
            {
                "pattern": r"find total (.+?) (?:broken down by|by) (.+)",
                "pattern_name": "total_A_by_B",
                "variables": ["A", "B"]
            },
            {
                "pattern": r"(?:list|show|find|get) (?:all )?(?:the )?(.+?) where (.+?) (is|=|equals) '(.+?)'",
                "pattern_name": "select_where",
                "variables": ["Entity", "Field", "Value"]
            },
            {
                "pattern": r"list distinct (.+?) values",
                "pattern_name": "distinct_values",
                "variables": ["A"]
            },
            {
                "pattern": r"get records ordered by (.+?) in (ascending|descending) order",
                "pattern_name": "order_by",
                "variables": ["A", "order"]
            },
            {
                "pattern": r"find records where (.+?) between (.+?) and (.+)",
                "pattern_name": "between",
                "variables": ["A", "start", "end"]
            },
            # Add more patterns as needed
        ]
    elif db_type.lower() == "mongodb":
        patterns = [
            {
                # Matches: "Find all entities where field is 'value'"
                "pattern": r"find all (.+?) (?:where|with|having) (.+?) (is|=|equals) '(.+?)'",
                "pattern_name": "find_where_entity",
                "variables": ["Entity", "Field", "Operator", "Value"]
            },
            {
                # Matches: "Find entities with field 'value'"
                "pattern": r"find (?:all )?(.+?) (?:with|having) (.+?) '(.+?)'",
                "pattern_name": "find_where_entity_no_operator",
                "variables": ["Entity", "Field", "Value"]
            },
            {
                # Matches: "Sort entities by field"
                "pattern": r"(?:sort|order) (?:all )?(.+?) by (.+)",
                "pattern_name": "find_sort",
                "variables": ["Entity", "Field"]
            },
            {
                # Matches: "Find documents where field is value"
                "pattern": r"find documents where (.+?) (is|=|equals) '(.+?)'",
                "pattern_name": "find_where",
                "variables": ["Field", "Operator", "Value"]
            },
            {
                # Matches: "Insert a document with field1: value1, field2: value2"
                "pattern": r"insert a document with (.+)",
                "pattern_name": "insert_one",
                "variables": ["Fields"]
            },
            {
                # Matches: "Update documents where field is value set field to value"
                "pattern": r"update documents where (.+?) (is|=|equals) '(.+?)' set (.+?) to '(.+?)'",
                "pattern_name": "update_one",
                "variables": ["FilterField", "Operator", "FilterValue", "UpdateField", "UpdateValue"]
            },
            {
                # Matches: "Delete documents where field is value"
                "pattern": r"delete documents where (.+?) (is|=|equals) '(.+?)'",
                "pattern_name": "delete_many",
                "variables": ["Field", "Operator", "Value"]
            },
            {
                # Matches: "Find total A by B"
                "pattern": r"find total (.+?) (?:broken down by|by) (.+)",
                "pattern_name": "total_A_by_B",
                "variables": ["A", "B"]
            },
            # Add more patterns as needed
        ]
    else:
        return None

    for p in patterns:
        match = re.match(p["pattern"], question_text)
        if match:
            variables = {}
            for i, var in enumerate(p["variables"]):
                variables[var] = match.group(i+1).strip("'\" ")
            return {"pattern_name": p["pattern_name"], "variables": variables}
    return None

def generate_query(parsed_question, db_type, template_instance):
    """
    Generate the database query based on the parsed question and database type.
    """
    pattern_name = parsed_question["pattern_name"]
    variables = parsed_question["variables"]

    if db_type.lower() == "mysql":
        if pattern_name == "select_where":
            Entity = variables["Entity"]
            Field = variables["Field"]
            Value = variables["Value"]

            matches_table = find_tables_matching(template_instance, Entity)
            matches_field = find_columns_matching(template_instance, Field)

            # Find common table that contains the field
            for table in matches_table:
                if any(t == table for t, c in matches_field):
                    col_Field = [c for t, c in matches_field if t == table][0]
                    query = f"SELECT * FROM `{table}` WHERE `{col_Field}` = '{Value}';"
                    natural_language = f"Select records from {table} where {col_Field} = '{Value}'"
                    return {"query": query, "natural_language": natural_language}
            return None

        elif pattern_name == "select_where_operator":
            Entity = variables["Entity"]
            Field = variables["Field"]
            Operator = variables["Operator"]
            Value = variables["Value"]

            # Map operator words to SQL operators
            operator_map = {
                "greater than": ">",
                "less than": "<",
                ">": ">",
                "<": "<",
                ">=": ">=",
                "<=": "<=",
                "=": "=",
                "equals": "=",
                "!=": "!=",
                "not equal to": "!=",
            }
            Operator = operator_map.get(Operator.lower(), Operator)

            matches_table = find_tables_matching(template_instance, Entity)
            matches_field = find_columns_matching(template_instance, Field)

            # Find common table that contains the field
            for table in matches_table:
                if any(t == table for t, c in matches_field):
                    col_Field = [c for t, c in matches_field if t == table][0]
                    query = f"SELECT * FROM `{table}` WHERE `{col_Field}` {Operator} {Value};"
                    natural_language = f"Select records from {table} where {col_Field} {Operator} {Value}"
                    return {"query": query, "natural_language": natural_language}
            return None

        elif pattern_name == "total_A_by_B":
            A = variables["A"]
            B = variables["B"]
            # Find matching columns in the database
            matches_A = find_columns_matching(template_instance, A)
            matches_B = find_columns_matching(template_instance, B)
            # Find common tables that contain both columns
            common_tables = set([t for t, c in matches_A]) & set([t for t, c in matches_B])
            if common_tables:
                table = common_tables.pop()  # Choose one table
                col_A = [c for t, c in matches_A if t == table][0]
                col_B = [c for t, c in matches_B if t == table][0]
                query = f"SELECT `{col_B}`, SUM(`{col_A}`) as total_{col_A} FROM `{table}` GROUP BY `{col_B}`;"
                natural_language = f"Total {col_A} by {col_B}"
                return {"query": query, "natural_language": natural_language}
            else:
                return None

        elif pattern_name == "distinct_values":
            A = variables["A"]
            matches_A = find_columns_matching(template_instance, A)
            if matches_A:
                table = matches_A[0][0]
                col_A = matches_A[0][1]
                query = f"SELECT DISTINCT `{col_A}` FROM `{table}`;"
                natural_language = f"List distinct values of {col_A} from {table}"
                return {"query": query, "natural_language": natural_language}
            else:
                return None

        elif pattern_name == "order_by":
            A = variables["A"]
            order = variables["order"]
            order = "ASC" if order == "ascending" else "DESC"
            matches_A = find_columns_matching(template_instance, A)
            if matches_A:
                table = matches_A[0][0]
                col_A = matches_A[0][1]
                query = f"SELECT * FROM `{table}` ORDER BY `{col_A}` {order};"
                natural_language = f"Get records from {table} ordered by {col_A} in {order} order"
                return {"query": query, "natural_language": natural_language}
            else:
                return None

        elif pattern_name == "between":
            A = variables["A"]
            start = variables["start"]
            end = variables["end"]
            matches_A = find_columns_matching(template_instance, A)
            if matches_A:
                table = matches_A[0][0]
                col_A = matches_A[0][1]
                query = f"SELECT * FROM `{table}` WHERE `{col_A}` BETWEEN {start} AND {end};"
                natural_language = f"Find records from {table} where {col_A} between {start} and {end}"
                return {"query": query, "natural_language": natural_language}
            else:
                return None
        else:
            return None

    elif db_type.lower() == "mongodb":
        if pattern_name in ["find_where_entity", "find_where_entity_no_operator"]:
            Entity = variables.get("Entity")
            Field = variables.get("Field")
            Value = variables.get("Value")

            matches_collection = find_collections_matching(template_instance, Entity)
            matches_field = find_fields_matching(template_instance, Field)

            # Find common collection that contains the field
            for collection in matches_collection:
                if any(coll == collection for coll, field in matches_field):
                    field_db = [field for coll, field in matches_field if coll == collection][0]
                    query_params = {field_db: Value}
                    query_str = f"db.{collection}.find({json.dumps(query_params)})"
                    natural_language = f"Find documents in {collection} where {field_db} equals '{Value}'"
                    return {"query": query_str, "natural_language": natural_language}
            return None

        elif pattern_name == "find_sort":
            Entity = variables["Entity"]
            Field = variables["Field"]
            matches_collection = find_collections_matching(template_instance, Entity)
            matches_field = find_fields_matching(template_instance, Field)
            # Find collection that contains the field
            for collection in matches_collection:
                if any(coll == collection for coll, field in matches_field):
                    field_db = [field for coll, field in matches_field if coll == collection][0]
                    query_str = f"db.{collection}.find().sort({{{field_db}: 1}})"
                    natural_language = f"Find documents in {collection} sorted by {field_db}"
                    return {"query": query_str, "natural_language": natural_language}
            return None
        elif pattern_name == "find_where":
            Field = variables["Field"]
            Operator = variables["Operator"]
            Value = variables["Value"]
            matches_field = find_fields_matching(template_instance, Field)
            if matches_field:
                collection = matches_field[0][0]
                field_db = matches_field[0][1]
                query_params = {field_db: Value}
                query_str = f"db.{collection}.find({json.dumps(query_params)})"
                natural_language = f"Find documents in {collection} where {field_db} equals '{Value}'"
                return {"query": query_str, "natural_language": natural_language}
            else:
                return None

        elif pattern_name == "insert_one":
            Fields = variables["Fields"]
            # Assume fields are in the format "field1: value1, field2: value2"
            field_pairs = [f.strip() for f in Fields.split(',')]
            document = {}
            for pair in field_pairs:
                if ':' in pair:
                    key, value = pair.split(':', 1)
                    document[key.strip()] = value.strip()
            # Use the first collection
            collection = template_instance.collections[0]
            query_str = f"db.{collection}.insertOne({json.dumps(document)})"
            natural_language = f"Insert a document into {collection} with fields {Fields}"
            return {"query": query_str, "natural_language": natural_language}

        elif pattern_name == "update_one":
            FilterField = variables["FilterField"]
            Operator = variables["Operator"]
            FilterValue = variables["FilterValue"]
            UpdateField = variables["UpdateField"]
            UpdateValue = variables["UpdateValue"]

            matches_filter = find_fields_matching(template_instance, FilterField)
            matches_update = find_fields_matching(template_instance, UpdateField)

            # Find common collections
            common_collections = set([t for t, c in matches_filter]) & set([t for t, c in matches_update])
            if common_collections:
                collection = common_collections.pop()
                filter_field_db = [c for t, c in matches_filter if t == collection][0]
                update_field_db = [c for t, c in matches_update if t == collection][0]
                query_params = {filter_field_db: FilterValue}
                update_params = {"$set": {update_field_db: UpdateValue}}
                query_str = f"db.{collection}.updateOne({json.dumps(query_params)}, {json.dumps(update_params)})"
                natural_language = f"Update documents in {collection} where {filter_field_db} equals '{FilterValue}', set {update_field_db} to '{UpdateValue}'"
                return {"query": query_str, "natural_language": natural_language}
            else:
                return None

        elif pattern_name == "delete_many":
            Field = variables["Field"]
            Operator = variables["Operator"]
            Value = variables["Value"]
            matches_field = find_fields_matching(template_instance, Field)
            if matches_field:
                collection = matches_field[0][0]
                field_db = matches_field[0][1]
                query_params = {field_db: Value}
                query_str = f"db.{collection}.deleteMany({json.dumps(query_params)})"
                natural_language = f"Delete documents from {collection} where {field_db} equals '{Value}'"
                return {"query": query_str, "natural_language": natural_language}
            else:
                return None

        elif pattern_name == "total_A_by_B":
            A = variables["A"]
            B = variables["B"]
            # Find matching fields in the database
            matches_A = find_fields_matching(template_instance, A)
            matches_B = find_fields_matching(template_instance, B)
            # Find common collections that contain both fields
            common_collections = set([t for t, c in matches_A]) & set([t for t, c in matches_B])
            if common_collections:
                collection = common_collections.pop()  # Choose one collection
                field_A = [c for t, c in matches_A if t == collection][0]
                field_B = [c for t, c in matches_B if t == collection][0]
                pipeline = [
                    {"$group": {"_id": f"${field_B}", f"total_{field_A}": {"$sum": f"${field_A}"}}}
                ]
                query_str = f"db.{collection}.aggregate({json.dumps(pipeline)})"
                natural_language = f"Total {field_A} by {field_B}"
                return {"query": query_str, "natural_language": natural_language}
            else:
                return None
        else:
            return None
    else:
        return None

def find_collections_matching(template_instance, collection_name):
    """
    Find collections in the database that match the given collection name.
    """
    matches = []
    for collection in template_instance.collections:
        if collection_name.lower() == collection.lower():
            matches.append(collection)
        elif collection_name.lower() in collection.lower():
            matches.append(collection)
    return matches

def find_fields_matching(template_instance, field_name):
    """
    Find fields in the database that match the given field name.
    """
    matches = []
    for collection in template_instance.collections:
        fields = template_instance.get_collection_fields(collection)
        for field in fields:
            if field_name.lower() == field.lower():
                matches.append((collection, field))
            elif field_name.lower() in field.lower():
                matches.append((collection, field))
    return matches

def find_columns_matching(template_instance, column_name):
    """
    Find columns in the database that match the given column name.
    """
    matches = []
    for table in template_instance.tables:
        columns = template_instance.get_column_names(table)
        for col in columns:
            if column_name.lower() == col.lower():
                matches.append((table, col))
            elif column_name.lower() in col.lower():
                matches.append((table, col))
    return matches

def find_tables_matching(template_instance, table_name):
    """
    Find tables in the database that match the given table name.
    """
    matches = []
    for table in template_instance.tables:
        if table_name.lower() == table.lower():
            matches.append(table)
        elif table_name.lower() in table.lower():
            matches.append(table)
    return matches

def find_fields_matching(template_instance, field_name):
    """
    Find fields in the database that match the given field name.
    """
    matches = []
    for collection in template_instance.collections:
        fields = template_instance.get_collection_fields(collection)
        for field in fields:
            if field_name.lower() == field.lower():
                matches.append((collection, field))
            elif field_name.lower() in field.lower():
                matches.append((collection, field))
    return matches

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
            template.template_insert_one,
            template.template_insert_many,
            template.template_find,
            template.template_regex,
            template.template_math_operations,
        ]
    else:
        return []

    random_queries = []
    for _ in range(count):
        template_func = random.choice(templates)
        try:
            query = template_func()["query"] if isinstance(template, MySQLTemplate) else template_func()["query_str"]
            random_queries.append(query)
        except Exception as e:
            print(f"Error generating random query: {e}")
            continue

    return random_queries

def parse_raw_mongo_query(raw_query):
    """
    Parse raw MongoDB shell-style query into the structure expected by execute_query.
    """
    try:
        if not raw_query.startswith("db."):
            raise ValueError("Query does not start with 'db.'")

        # Extract collection and operation
        parts = raw_query.split(".", 2)
        if len(parts) < 2:
            raise ValueError("Invalid MongoDB query format")

        collection = parts[1].split("(", 1)[0].strip()  # Extract collection name

        # Detect the query type
        if "find(" in raw_query:
            params = raw_query.split("find(", 1)[1].rstrip(")")
            query_params, projection = "{}", "{}"  # Defaults

            if "," in params:
                # Split into query parameters and projection
                query_params, projection = map(str.strip, params.split(",", 1))
            else:
                query_params = params.strip()

            query_params = ast.literal_eval(query_params) if query_params else {}
            projection = ast.literal_eval(projection) if projection else {}

            return {
                "query_type": "find",
                "collection": collection,
                "query_params": query_params,
                "query_projection": projection,
            }

        elif "insertOne(" in raw_query:
            params = raw_query.split("insertOne(", 1)[1].rstrip(")")
            document = ast.literal_eval(params)
            return {
                "query_type": "insertOne",
                "collection": collection,
                "query_params": document,
            }

        elif "insertMany(" in raw_query:
            params = raw_query.split("insertMany(", 1)[1].rstrip(")")
            documents = ast.literal_eval(params)
            return {
                "query_type": "insertMany",
                "collection": collection,
                "query_params": documents,
            }

        elif "updateOne(" in raw_query:
            params = raw_query.split("updateOne(", 1)[1].rstrip(")")
            filter_query, update_query = map(str.strip, params.split(",", 1))
            filter_query = ast.literal_eval(filter_query)
            update_query = ast.literal_eval(update_query)

            return {
                "query_type": "updateOne",
                "collection": collection,
                "query_params": filter_query,
                "update_params": update_query,
            }

        elif "deleteMany(" in raw_query:
            params = raw_query.split("deleteMany(", 1)[1].rstrip(")")
            query = ast.literal_eval(params)

            return {
                "query_type": "deleteMany",
                "collection": collection,
                "query_params": query,
            }

        elif "aggregate(" in raw_query:
            params = raw_query.split("aggregate(", 1)[1].rstrip(")")
            pipeline = ast.literal_eval(params)

            return {
                "query_type": "aggregate",
                "collection": collection,
                "pipeline": pipeline,
            }

        else:
            raise ValueError("Unsupported query type")

    except Exception as e:
        raise ValueError(f"Failed to parse MongoDB query: {str(e)}")

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
            template = MySQLTemplate(db_name)
            result = template.execute_query(query)
            return jsonify({"result": result}), 200

        elif db_type.lower() == "mongodb":
            try:
                if db_name == "Weather":
                    db_name = "sample_weatherdata"
                elif db_name != "userdatabase":
                    db_name = db_name if db_name == "userdatabase" else "sample_" + db_name.lower()

                template = MongoDBTemplate(db_name)
                transformed_query = parse_raw_mongo_query(query)
                result = template.execute_query(transformed_query)

                # Convert MongoDB result to JSON-serializable format
                serializable_result = make_json_serializable(result)

                # Handle specific query type responses
                if transformed_query["query_type"] in ["insertOne", "insertMany"]:
                    return jsonify({"result": str(serializable_result)}), 200
                else:
                    return jsonify({"result": serializable_result}), 200

            except Exception as e:
                print(f"Error executing MongoDB query: {e}")
                return jsonify({"error": f"Failed to execute MongoDB query: {str(e)}"}), 500

        else:
            return jsonify({"error": f"Unsupported database type: {db_type}"}), 400

    except Exception as e:
        print(f"Unexpected error executing query: {e}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500