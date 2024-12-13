from pymongo import MongoClient
from config.mongo_config import MONGO_URI
from bson.decimal128 import Decimal128

client = MongoClient(MONGO_URI)

def get_mongo_db(db_name):
    """Get a MongoDB database instance."""
    return client[db_name]

def decimal128_to_json_serializable(data):
    """Recursively convert Decimal128 objects to float or str in a data structure."""
    if isinstance(data, list):
        return [decimal128_to_json_serializable(item) for item in data]
    elif isinstance(data, dict):
        return {key: decimal128_to_json_serializable(value) for key, value in data.items()}
    elif isinstance(data, Decimal128):
        # Convert Decimal128 to a JSON-serializable type (float or str)
        return float(data.to_decimal())
    return data

def fetch_mongo_collections_with_sample_data(db_name):
    """Fetch up to 5 collections and their sample data from MongoDB."""
    db = get_mongo_db(db_name)
    collections = db.list_collection_names()[:5]  # Limit to 5 collections

    result = []
    for collection_name in collections:
        collection = db[collection_name]
        sample_data = list(collection.find({}, {"_id": 0}).limit(5))  # Limit sample data to 5 documents
        fields = sample_data[0].keys() if sample_data else []

        result.append({
            "name": collection_name,
            "fields": list(fields),
            "sampleData": sample_data
        })

    return result