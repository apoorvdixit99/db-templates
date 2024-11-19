from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import random
import json

USER = 'root'
PASSWORD = 'mongodb'
DATABASE = 'sample_airbnb'
URI = """mongodb+srv://{}:{}@cluster0.jgquf.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0""".format(
                        USER, PASSWORD)

class MongoDBTemplate:

    def __init__(self):
        self.uri = URI
        self.client = MongoClient(self.uri, server_api=ServerApi('1'))
        self.db = self.client[DATABASE]
        self.collections = self.db.list_collection_names()
        self.math_operations = ['$gte', '$gt', '$lte', '$lt', '$eq', '$ne']
    
    def ping_server(self):
        try:
            self.client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print(e)

    # Helper
    def generate_sample_document(self):
        sample_document = {}
        for i in range(random.randint(1, 6)):
            sample_document[f"key{i}"] = f"val{i}"
        return sample_document
    
    def generate_sample_documents(self):
        sample_documents = []
        for i in range(random.randint(3, 6)):
            sample_document = {}
            for j in range(random.randint(1, 6)):
                sample_document[f"key{i}{j}"] = f"val{i}{j}"
            sample_documents.append(sample_document)
        return sample_documents
    
    def execute_query(self, query):
        collection_name = query['collection']
        collection = self.db[collection_name]
        def default_case():
            return "Invalid Query"
        switch = {
            "insertOne": collection.insert_one,
            "insertMany": collection.insert_many,
            "find": collection.find
        }
        query_func = switch.get(
            query['query_type'], 
            default_case
        )
        if 'query_projection' not in query.keys():
            result = query_func(query['query_params'])
        else:
            result = query_func(query['query_params'], query['query_projection'])
        return result
    
    def get_attribute_types(self, collection_name):
        result = {}
        collection = self.db[collection_name]
        first_document = collection.find_one()
        for key, value in first_document.items():
            result[key] = type(value).__name__
        return result


    # Templates

    def template_insert_one(self):
        document = self.generate_sample_document()
        collection = random.choice(self.collections)
        doc_str = json.dumps(document)
        query_type = 'insertOne'
        query_str = f"""db.{collection}.{query_type}({doc_str})"""
        return {
            'query_type': query_type,
            'query_params': document,
            'collection': collection,
            'query_str': query_str
        }

    def template_insert_many(self):
        documents = self.generate_sample_documents()
        collection = random.choice(self.collections)
        doc_str = json.dumps(documents)
        query_type = 'insertMany'
        query_str = f"""db.{collection}.{query_type}({doc_str})"""
        return {
            'query_type': query_type,
            'query_params': documents,
            'collection': collection,
            'query_str': query_str
        }
    
    def template_find(self):
        collection = random.choice(self.collections)
        query_type = 'find'
        query_params = ""
        query_str = f"""db.{collection}.{query_type}({query_params})"""
        return {
            'query_type': query_type,
            'query_params': query_params,
            'collection': collection,
            'query_str': query_str
        }
    
    def template_math_operations(self):
        collection = random.choice(self.collections)
        query_type = 'find'

        attributes = self.get_attribute_types(collection)
        int_attributes = [key for key, value in attributes.items() if value == 'int']
        attr = random.choice(int_attributes)
        unique_values = self.db[collection].distinct(attr)
        unique_values = list(unique_values)
        value = random.choice(unique_values)
        op = random.choice(self.math_operations)

        query_params = {
            attr: {
                op: value
            }
        }
        query_projection = {
            attr: 1,
            '_id': 1
        }
        query_str = f"""db.{collection}.{query_type}({query_params},{query_projection})"""
        return {
            'query_type': query_type,
            'query_params': query_params,
            'collection': collection,
            'query_str': query_str,
            'query_projection': query_projection
        }
    
    
    def template_regex(self):
        collection = random.choice(self.collections)
        query_type = 'find'

        attributes = self.get_attribute_types(collection)
        str_attributes = [key for key, value in attributes.items() if value == 'str']
        attr = random.choice(str_attributes)
        unique_values = self.db[collection].distinct(attr)
        unique_values = list(unique_values)
        value = random.choice(unique_values)
        value = value.split(' ')
        value = random.choice(value)
        case_insensitive = random.choice([0,1])

        if case_insensitive is 0:
            query_params = {
                attr: {
                    "$regex": value
                }
            }
            query_params_str = {
                attr: f'/{value}/'
            }
        else:
            query_params = {
                attr: {
                    "$regex": value,
                    "$options": "i"
                }
            }
            query_params_str = {
                attr: f'/{value}/i'
            }
        query_projection = {
            attr: 1,
            '_id': 1
        }
        query_str = f"""db.{collection}.{query_type}({query_params_str},{query_projection})"""
        return {
            'query_type': query_type,
            'query_params': query_params,
            'collection': collection,
            'query_str': query_str,
            'query_projection': query_projection
        }