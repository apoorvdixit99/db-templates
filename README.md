# README

## Project Overview
This project provides a Flask-based API service that generates and executes database queries for MySQL and MongoDB based on natural language inputs. It includes functionality to dynamically identify database templates, collections, fields, and values from user inputs.

## File Structure

```
.
├── app.py                   # Entry point to run the Flask application
├── routes
│   ├── chat.py              # Contains API routes for handling MySQL and MongoDB queries
│   ├── fetch_data.py        # Contains API routes for fetching data present in the database
│   ├── upload.py            # Contains API routes for creating database from the dataset uploaded by user
│   ├── __init__.py          # Initializes the routes module
├── services
│   ├── mongo_service.py      # Provides utility functions for MongoDB
│   ├── mongodb_template.py   # Provides MongoDB template generation logic
│   ├── mysql_query_generator.py  # Provides MySQL template generation logic
│   ├── mysql_service.py      # Provides utility functions for MySQL
├── requirements.txt         # List of Python dependencies for the project
├── README.md                # This file describing the project and its usage
```

### Directory Descriptions

- **`routes/`**: Contains Flask blueprints for different routes, such as `chat.py` for query handling.
- **`services/`**: Includes service logic, such as generating MySQL, MongoDB query templates.
- **`requirements.txt`**: Lists all Python dependencies required to run the project.

## Commands to Run the Project

### 1. Install Dependencies
Ensure you have Python installed (version 3.7 or later). Run the following command to install dependencies:
```bash
pip install -r requirements.txt
```

### 2. Run the Flask Application
Start the Flask application using the following command:
```bash
python app.py
```

By default, the application will run on `http://127.0.0.1:5000`. You can test the API endpoints using tools like Postman or `curl`.

### 3. API Endpoints

#### `/api/ask-question`
- **Method**: `POST`
- **Description**: Generates sample queries for MySQL or MongoDB based on user input.
- **Payload**:
  ```json
  {
      "question": "Find all restaurants serving Italian cuisine",
      "dbName": "your_database_name",
      "dbType": "mongodb"
  }
  ```
- **Response**:
  ```json
  {
      "template": "find projection",
      "collection": "restaurants",
      "field": "cuisine",
      "value": "Italian",
      "message": "success"
  }
  ```

#### `/api/run-query`
- **Method**: `POST`
- **Description**: Executes a query on MySQL or MongoDB and returns the result.
- **Payload**:
  ```json
  {
      "query": {"find": {"field": "cuisine", "value": "Italian"}},
      "dbName": "your_database_name",
      "dbType": "mongodb"
  }
  ```
- **Response**:
  ```json
  {
      "result": [{"name": "Restaurant A"}, {"name": "Restaurant B"}],
      "message": "success"
  }
  ```

## Additional Notes

- Ensure the database is configured and accessible by the application.
- Adjust the database connection settings in the respective templates or service files as needed.

For any issues or queries, feel free to contact the project maintainer.

