from flask import Flask, jsonify
from mysql_template import MySQLTemplate

app = Flask(__name__)

# Initialize MySQLTemplate
# Initialize MySQLTemplate
def get_mysql_template():
    global mysql_template
    if 'mysql_template' not in globals():
        mysql_template = MySQLTemplate()
    return mysql_template

@app.route('/')
def home():
    return jsonify({"message": "Welcome to SQL API"}), 200

# Route for basic SELECT
@app.route('/select/<int:limit>', methods=['GET'])
def select(limit):
    result = get_mysql_template().template_select(limit)
    return jsonify(result), 200

# Route for LIMIT query
@app.route('/limit', methods=['GET'])
def limit():
    result = get_mysql_template().template_limit()
    return jsonify(result), 200

# Route for DISTINCT
@app.route('/distinct/<int:limit>', methods=['GET'])
def distinct(limit):
    result = get_mysql_template().template_distinct(limit)
    return jsonify(result), 200

# Route for WHERE
@app.route('/where', methods=['GET'])
def where():
    result = get_mysql_template().template_where()
    return jsonify(result), 200

# Route for ORDER BY
@app.route('/order-by', methods=['GET'])
def order_by():
    result = get_mysql_template().template_order_by()
    return jsonify(result), 200

# Route for GROUP BY
@app.route('/group-by', methods=['GET'])
def group_by():
    result = get_mysql_template().template_group_by()
    return jsonify(result), 200

# Route for JOIN
@app.route('/join', methods=['GET'])
@app.route('/join/<join_type>', methods=['GET'])
def join(join_type=''):
    result = get_mysql_template().template_join(join=join_type)
    return jsonify(result), 200

# Route for IN construct
@app.route('/in', methods=['GET'])
def in_construct():
    result = get_mysql_template().template_in()
    return jsonify(result), 200

# Route for BETWEEN
@app.route('/between', methods=['GET'])
def between():
    result = get_mysql_template().template_between()
    return jsonify(result), 200

# Route for HAVING
@app.route('/having', methods=['GET'])
def having():
    result = get_mysql_template().template_having()
    return jsonify(result), 200

# Cleanup resources when the app shuts down
@app.teardown_appcontext
def close_connection(exception):
    mysql_template = get_mysql_template()  # Explicitly declare it as global
    if 'mysql_template' in globals() and mysql_template:
        del mysql_template

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5050)
