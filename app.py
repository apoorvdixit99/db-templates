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