import random
import json
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from config.mongo_config import MONGO_URI
from bson import ObjectId

class MongoDBTemplate:
    def __init__(self, db_name, uri="mongodb://localhost:27017/"):
        self.client = MongoClient(MONGO_URI, server_api=ServerApi('1'))
        self.db = self.client[db_name]
        self.collections = self.db.list_collection_names()
        self.math_operations = ['$gte', '$gt', '$lte', '$lt', '$eq', '$ne']

        if not self.collections:
            raise ValueError(f"Database '{db_name}' has no collections.")

    def __del__(self):
        if self.client:
            self.client.close()

    # Helper Functions

    def get_collection_fields(self, collection_name):
        document = self.db[collection_name].find_one()
        if not document:
            return []
        return list(document.keys())

    def get_field_types(self, collection_name):
        document = self.db[collection_name].find_one()
        if not document:
            return {}
        return {key: type(value).__name__ for key, value in document.items()}

    def get_field_values(self, collection_name, field, limit=10):
        return list(self.db[collection_name].distinct(field))[:limit]

    # Query Templates

    def template_find(self):
        collection = random.choice(self.collections)
        query_str = f"db.{collection}.find()"
        return {"query_str": query_str, "message": "success"}

    def template_find_with_projection(self):
        collection = random.choice(self.collections)
        fields = self.get_collection_fields(collection)
        if not fields:
            print(fields + "Here, will return empty")
            return {"query_str": "", "message": "No fields available for projection"}

        projection = {field: 1 for field in random.sample(fields, min(len(fields), 3))}
        query_str = f"db.{collection}.find({{}}, {json.dumps(projection)})"
        return {"query_str": query_str, "message": "success"}

    def template_regex(self):
        collection = random.choice(self.collections)
        field_types = self.get_field_types(collection)
        string_fields = [key for key, value in field_types.items() if value == 'str']

        if not string_fields:
            return {"query_str": "", "message": "No string fields available for regex search"}

        field = random.choice(string_fields)
        values = self.get_field_values(collection, field)
        if not values:
            return {"query_str": "", "message": "No values found for regex"}

        regex_value = values[0][:3]
        query = {field: {"$regex": regex_value, "$options": "i"}}
        query_str = f"db.{collection}.find({json.dumps(query)})"
        return {"query_str": query_str, "message": "success"}

    def template_math_operations(self):
        collection = random.choice(self.collections)
        attributes = self.get_field_types(collection)
        numeric_fields = [key for key, value in attributes.items() if value in ["int", "float"]]

        if not numeric_fields:
            return {"query_str": "", "message": "No numeric fields available for math operations"}

        attr = random.choice(numeric_fields)
        unique_values = self.get_field_values(collection, attr)
        if not unique_values:
            return {"query_str": "", "message": f"No values found for field '{attr}'"}

        value = random.choice(unique_values)
        operator = random.choice(self.math_operations)
        query_params = {attr: {operator: value}}
        query_projection = {attr: 1, "_id": 1}
        query_str = f"db.{collection}.find({json.dumps(query_params)}, {json.dumps(query_projection)})"

        return {"query_str": query_str, "message": "success"}

    def template_insert_one(self):
        collection = random.choice(self.collections)
        document = {f"key{i}": f"value{i}" for i in range(1, random.randint(2, 6))}
        query_str = f"db.{collection}.insertOne({json.dumps(document)})"
        return {"query_str": query_str, "message": "success"}

    def template_insert_many(self):
        collection = random.choice(self.collections)
        documents = [{f"key{i}{j}": f"value{i}{j}" for j in range(1, random.randint(2, 4))} for i in range(1, random.randint(3, 6))]
        query_str = f"db.{collection}.insertMany({json.dumps(documents)})"
        return {"query_str": query_str, "message": "success"}

    def template_aggregate(self):
        collection = random.choice(self.collections)
        fields = self.get_collection_fields(collection)
        if not fields:
            return {"query_str": "", "message": "No fields available for aggregation"}

        group_field = random.choice(fields)
        pipeline = [
            {"$group": {"_id": f"${group_field}", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10},
        ]
        query_str = f"db.{collection}.aggregate({json.dumps(pipeline)})"
        return {"query_str": query_str, "message": "success"}

    # def execute_query(self, query):
    #     """
    #     Execute a MongoDB query and return the result.
    #     """
    #     try:
    #         collection = self.db[query["collection"]]
    #         if query["query_type"] == "find":
    #             # Apply transformation to handle ObjectId
    #             documents = collection.find(query.get("query_params", {}), query.get("query_projection", {})).limit(10)
    #             return [self.transform_document(doc) for doc in documents]
    #         elif query["query_type"] == "insertOne":
    #             return collection.insert_one(query["query_params"]).inserted_id
    #         elif query["query_type"] == "insertMany":
    #             return collection.insert_many(query["query_params"]).inserted_ids
    #         else:
    #             return "Query type not supported"
    #     except Exception as e:
    #         print(f"Error executing query: {e}")
    #         return {"error": str(e)}

    def execute_query(self, query):
        """
        Execute a MongoDB query and return the result.
        """
        try:
            collection = self.db[query["collection"]]

            if query["query_type"] == "find":
                documents = collection.find(query.get("query_params", {}), query.get("query_projection", {})).limit(10)
                return [self.transform_document(doc) for doc in documents]

            elif query["query_type"] == "insertOne":
                result = collection.insert_one(query["query_params"])
                return result.inserted_id

            elif query["query_type"] == "insertMany":
                result = collection.insert_many(query["query_params"])
                return result.inserted_ids

            elif query["query_type"] == "updateOne":
                result = collection.update_one(query["query_params"], query["update_params"])
                return {"matched_count": result.matched_count, "modified_count": result.modified_count}

            elif query["query_type"] == "deleteMany":
                result = collection.delete_many(query["query_params"])
                return {"deleted_count": result.deleted_count}

            else:
                raise ValueError("Unsupported query type")

        except Exception as e:
            print(f"Error executing query: {e}")
            raise

        
    def transform_document(self, document):
        """
        Transform a MongoDB document to make it JSON serializable.
        """
        if isinstance(document, dict):
            for key, value in document.items():
                if isinstance(value, ObjectId):
                    document[key] = str(value)
                elif isinstance(value, list):
                    document[key] = [self.transform_document(v) if isinstance(v, (dict, list)) else v for v in value]
        return document

    def template_update_one(self):
        collection = random.choice(self.collections)
        fields = self.get_collection_fields(collection)
        if not fields:
            return {"query_str": "", "message": "No fields available for update"}

        field_to_update = random.choice(fields)
        update_value = f"new_value_{random.randint(1, 100)}"
        filter_field = random.choice(fields)
        filter_value = random.choice(self.get_field_values(collection, filter_field)) if self.get_field_values(collection, filter_field) else None

        if not filter_value:
            return {"query_str": "", "message": "No values available for update filter"}

        query = {filter_field: filter_value}
        update = {"$set": {field_to_update: update_value}}
        query_str = f"db.{collection}.updateOne({json.dumps(query)}, {json.dumps(update)})"
        return {"query_str": query_str, "message": "success"}

    def template_delete_many(self):
        collection = random.choice(self.collections)
        fields = self.get_collection_fields(collection)
        if not fields:
            return {"query_str": "", "message": "No fields available for deletion"}

        field = random.choice(fields)
        values = self.get_field_values(collection, field)
        if not values:
            return {"query_str": "", "message": "No values available for deletion"}

        filter_value = random.choice(values)
        query = {field: filter_value}
        query_str = f"db.{collection}.deleteMany({json.dumps(query)})"
        return {"query_str": query_str, "message": "success"}

    def template_count_documents(self):
        collection = random.choice(self.collections)
        fields = self.get_collection_fields(collection)
        if not fields:
            return {"query_str": "", "message": "No fields available for count"}

        field = random.choice(fields)
        values = self.get_field_values(collection, field)
        if not values:
            return {"query_str": "", "message": "No values available for count"}

        filter_value = random.choice(values)
        query = {field: filter_value}
        query_str = f"db.{collection}.countDocuments({json.dumps(query)})"
        return {"query_str": query_str, "message": "success"}
    
    def template_find_or(self):
        collection = random.choice(self.collections)
        fields = self.get_collection_fields(collection)
        if len(fields) < 2:
            return {"query_str": "", "message": "Not enough fields for $or condition"}

        or_conditions = []
        for field in random.sample(fields, 2):  # Pick two fields for $or condition
            values = self.get_field_values(collection, field)
            if values:
                or_conditions.append({field: random.choice(values)})

        if not or_conditions:
            return {"query_str": "", "message": "No values available for $or condition"}

        query = {"$or": or_conditions}
        query_str = f"db.{collection}.find({json.dumps(query)})"
        return {"query_str": query_str, "message": "success"}
    
    def template_find_and_sort(self):
        collection = random.choice(self.collections)
        field_types = self.get_field_types(collection)
        sortable_fields = [key for key, value in field_types.items() if value in ["int", "float", "str"]]

        if not sortable_fields:
            return {"query_str": "", "message": "No fields available for sorting"}

        sort_field = random.choice(sortable_fields)
        sort_order = random.choice([1, -1])  # 1 for ascending, -1 for descending
        query_str = f"db.{collection}.find().sort({{{sort_field}: {sort_order}}})"
        return {"query_str": query_str, "message": "success"}
