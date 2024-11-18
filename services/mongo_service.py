from pymongo import MongoClient
from config.mongo_config import MONGO_URI

client = MongoClient(MONGO_URI)

def get_mongo_db(db_name):
    """Get a MongoDB database instance."""
    return client[db_name]

def fetch_mongo_collections_with_sample_data(db_name):
    """Fetch collections and sample data from MongoDB."""
    db = get_mongo_db(db_name)
    collections = db.list_collection_names()

    result = []
    for collection_name in collections:
        collection = db[collection_name]
        sample_data = list(collection.find({}, {"_id": 0}).limit(5))
        fields = sample_data[0].keys() if sample_data else []

        result.append({"name": collection_name, "fields": list(fields), "sampleData": sample_data})

    return result