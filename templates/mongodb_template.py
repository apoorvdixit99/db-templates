from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import random
from config.mongo_config import MONGO_URI
import json

USER = 'root'
PASSWORD = 'mongodb'
DATABASE = 'sample_airbnb'
URI = """mongodb+srv://{}:{}@cluster0.jgquf.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0""".format(
                        USER, PASSWORD)

class MongoDBTemplate:

    def __init__(self, db_name, uri="mongodb://localhost:27017/"):
        self.uri = URI
        # self.client = MongoClient(self.uri, server_api=ServerApi('1'))
        # self.db = self.client[DATABASE]
        # self.collections = self.db.list_collection_names()
        self.client = MongoClient(MONGO_URI, server_api=ServerApi('1'))
        self.db = self.client[db_name]
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
    
    def get_all_collections(self):
        return self.collections
    
    # def execute_query(self, query):
    #     collection_name = query['collection']
    #     collection = self.db[collection_name]
    #     def default_case():
    #         return "Invalid Query"
    #     switch = {
    #         "insertOne": collection.insert_one,
    #         "insertMany": collection.insert_many,
    #         "find": collection.find
    #     }
    #     query_func = switch.get(
    #         query['query_type'], 
    #         default_case
    #     )
    #     if 'query_projection' not in query.keys():
    #         result = query_func(query['query_params'])
    #     else:
    #         result = query_func(query['query_params'], query['query_projection'])
    #     return result

    def execute_query(self, query):
        collection_name = query['collection']
        collection = self.db[collection_name]

        def default_case():
            raise ValueError("Invalid Query Type")

        # Define query type handlers
        switch = {
            "insertOne": collection.insert_one,
            "insertMany": collection.insert_many,
            "find": collection.find
        }

        # Select the appropriate query function
        query_func = switch.get(query['query_type'], default_case)

        try:
            # Execute the query based on the presence of projection
            if 'query_projection' not in query.keys():
                result = query_func(query['query_params'])
            else:
                result = query_func(query['query_params'], query['query_projection'])

            # Handle the result for each query type
            if query['query_type'] == "find":
                # Convert cursor to list for JSON serialization
                return list(result)
            elif query['query_type'] in ["insertOne", "insertMany"]:
                # Return insert acknowledgment as string
                return {"acknowledged": result.acknowledged, "inserted_ids": getattr(result, "inserted_ids", [result.inserted_id])}
            else:
                raise ValueError("Unsupported query type")

        except Exception as e:
            raise RuntimeError(f"Error executing query: {e}")
    
    def get_attribute_types(self, collection_name):
        result = {}
        collection = self.db[collection_name]
        first_document = collection.find_one()
        for key, value in first_document.items():
            result[key] = type(value).__name__
        return result
    
    def get_collection_fields(self, collection_name):
        document = self.db[collection_name].find_one()
        if not document:
            return []
        return list(document.keys())


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
    
    # def template_find_math_operations(self):
    #     collection = random.choice(self.collections)
    #     query_type = 'find'

    #     attributes = self.get_attribute_types(collection)
    #     int_attributes = [key for key, value in attributes.items() if value == 'int']
    #     attr = random.choice(int_attributes)
    #     unique_values = self.db[collection].distinct(attr)
    #     unique_values = list(unique_values)
    #     value = random.choice(unique_values)
    #     op = random.choice(self.math_operations)

    #     query_params = {
    #         attr: {
    #             op: value
    #         }
    #     }
    #     if op=='$ne':
    #         query_params[attr]['$exists'] = True
    #     query_projection = {
    #         attr: 1,
    #         '_id': 1
    #     }
    #     query_str = f"""db.{collection}.{query_type}({query_params},{query_projection})"""
    #     return {
    #         'query_type': query_type,
    #         'query_params': query_params,
    #         'collection': collection,
    #         'query_str': query_str,
    #         'query_projection': query_projection
    #     }

    def template_find_math_operations(self, collection=None, query_params=None):
        """
        Generate a MongoDB find query with math operations.
        If query_params are provided, use them; otherwise, generate random parameters.
        """
        # If no collection is provided, choose a random one
        collection = collection or random.choice(self.collections)
        query_type = 'find'

        if query_params:
            # Use provided query_params directly
            query_projection = {key: 1 for key in query_params.keys()}
            query_projection['_id'] = 1
            query_str = f"""db.{collection}.{query_type}({json.dumps(query_params)}, {json.dumps(query_projection)})"""
            return {
                'query_type': query_type,
                'query_params': query_params,
                'collection': collection,
                'query_str': query_str,
                'query_projection': query_projection
            }

        # If no query_params are provided, generate random parameters
        attributes = self.get_attribute_types(collection)  # Get attribute types for the collection
        int_attributes = [key for key, value in attributes.items() if value == 'int']  # Filter integer attributes

        if not int_attributes:
            return {
                'query_type': query_type,
                'query_params': {},
                'collection': collection,
                'query_str': "",
                'query_projection': {},
                'message': "No integer attributes available for math operations"
            }

        attr = random.choice(int_attributes)  # Choose a random integer attribute
        unique_values = self.db[collection].distinct(attr)  # Get distinct values for the attribute
        unique_values = [val for val in unique_values if isinstance(val, (int, float))]  # Ensure values are numeric

        if not unique_values:
            return {
                'query_type': query_type,
                'query_params': {},
                'collection': collection,
                'query_str': "",
                'query_projection': {},
                'message': "No numeric values available for math operations"
            }

        value = random.choice(unique_values)
        op = random.choice(self.math_operations)  # Choose a random operation (e.g., $gt, $lt, $ne)

        # Construct query parameters
        query_params = {
            attr: {
                op: value
            }
        }
        if op == '$ne':
            query_params[attr]['$exists'] = True

        query_projection = {
            attr: 1,
            '_id': 1
        }
        query_str = f"""db.{collection}.{query_type}({json.dumps(query_params)}, {json.dumps(query_projection)})"""

        return {
            'query_type': query_type,
            'query_params': query_params,
            'collection': collection,
            'query_str': query_str,
            'query_projection': query_projection
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
    
    # def template_find_with_projection(self):
    #     """Generate a MongoDB find query with random projection fields."""
    #     collection = random.choice(self.collections)  # Select a random collection
    #     fields = self.get_collection_fields(collection)  # Retrieve fields for the collection
        
    #     if not fields:
    #         print(fields + " - No fields available for projection")  # Debugging message
    #         return {
    #             'query_type': "find",
    #             'query_params': {},
    #             'collection': collection,
    #             'query_str': "",
    #             'query_projection': {}
    #         }

    #     # Generate a random projection with up to 3 fields
    #     projection = {field: 1 for field in random.sample(fields, min(len(fields), 3))}
        
    #     # Construct the query string
    #     query_str = f"db.{collection}.find({{}}, {json.dumps(projection)})"
    #     print("HERE!!!")
    #     # Return the correctly formatted response
    #     return {
    #         'query_type': "find",
    #         'query_params': {},  # No filter conditions specified in this template
    #         'collection': collection,
    #         'query_str': query_str,
    #         'query_projection': projection
    #     }

    # def template_find_with_projection(self, collection=None, field=None, value=None):
    #     """Generate a MongoDB find query with random projection fields."""
    #     if not collection:
    #         collection = random.choice(self.collections)  # Default to a random collection

    #     fields = self.get_collection_fields(collection)  # Retrieve fields for the collection

    #     if not fields:
    #         return {
    #             'query_type': "find",
    #             'query_params': {},
    #             'collection': collection,
    #             'query_str': "",
    #             'query_projection': {},
    #             'message': "No fields available for projection"
    #         }

    #     # Generate a random projection with up to 3 fields
    #     projection = {field: 1 for field in random.sample(fields, min(len(fields), 3))}
        
    #     # Construct the query string
    #     query_str = f"db.{collection}.find({{}}, {json.dumps(projection)})"

    #     return {
    #         'query_type': "find",
    #         'query_params': {},  # No filter conditions specified in this template
    #         'collection': collection,
    #         'query_str': query_str,
    #         'query_projection': projection,
    #         'message': "success"
    #     }

    def template_find_with_projection(self, collection=None, field=None, value=None):
        """Generate a MongoDB find query with random projection fields and optional filter."""
        if not collection:
            collection = random.choice(self.collections)  # Default to a random collection

        fields = self.get_collection_fields(collection)  # Retrieve fields for the collection

        if not fields:
            return {
                'query_type': "find",
                'query_params': {},
                'collection': collection,
                'query_str': "",
                'query_projection': {},
                'message': "No fields available for projection"
            }

        # Generate filter condition based on field and value
        query_params = {field: value} if field and value else {}

        # Generate a random projection with up to 3 fields
        projection = {field: 1 for field in random.sample(fields, min(len(fields), 3))}

        # Construct the query string
        query_str = f"db.{collection}.find({json.dumps(query_params)}, {json.dumps(projection)})"

        return {
            'query_type': "find",
            'query_params': query_params,
            'collection': collection,
            'query_str': query_str,
            'query_projection': projection,
            'message': "success"
        }


    
    
    def template_find_regex(self):
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

        if case_insensitive == 0:
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
    
    def template_find_regex_2(self):
        collection = random.choice(self.collections)
        query_type = 'find'

        attributes = self.get_attribute_types(collection)
        str_attributes = [key for key, value in attributes.items() if value == 'str']
        attr = random.choice(str_attributes)
        unique_values = self.db[collection].distinct(attr)
        unique_values = list(unique_values)
        value = random.choice(unique_values)
        value = value.split(' ')
        starts_with = random.choice([0,1])

        if starts_with == 1:
            value = value[0]
            query_params = {
                attr: {
                    "$regex": f"^{value}"
                }
            }
            query_params_str = {
                attr: f'/^{value}/'
            }
        else:
            value = value[-1]
            query_params = {
                attr: {
                    "$regex": f"{value}$"
                }
            }
            query_params_str = {
                attr: f'/{value}$/'
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
    
    def template_find_and(self):
        collection = random.choice(self.collections)
        query_type = 'find'

        attributes = self.get_attribute_types(collection)
        int_attributes = [key for key, value in attributes.items() if value == 'int']
        attr = random.choice(int_attributes)
        unique_values = self.db[collection].distinct(attr)
        unique_values = list(unique_values)
        value1, value2 = sorted(random.sample(unique_values, 2))

        query_params = {
            "$and": [
                {
                    attr: {
                        "$gte": value1
                    }
                },
                {
                    attr: {
                        "$lte": value2
                    }

                }
            ]
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
    
    def template_find_or(self):
        collection = random.choice(self.collections)
        query_type = 'find'

        attributes = self.get_attribute_types(collection)
        int_attributes = [key for key, value in attributes.items() if value == 'int']
        attr = random.choice(int_attributes)
        unique_values = self.db[collection].distinct(attr)
        unique_values = list(unique_values)
        value1, value2 = sorted(random.sample(unique_values, 2))

        query_params = {
            "$or": [
                {
                    attr: {
                        "$eq": value1
                    }
                },
                {
                    attr: {
                        "$eq": value2
                    }

                }
            ]
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

    def template_find_nested_attribute(self):
        collection = random.choice(self.collections)
        query_type = 'find'

        attributes = self.get_attribute_types(collection)
        dict_attributes = [key for key, value in attributes.items() if value == 'dict']
        attr = random.choice(dict_attributes)
        unique_values = self.db[collection].distinct(attr)
        unique_values = list(unique_values)
        value = random.choice(unique_values)
        nested_key = random.choice(list(value.keys()))
        nested_value = value[nested_key]

        query_params = {
            f"{attr}.{nested_key}": nested_value 
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

    def template_find_array_all(self):
        collection = random.choice(self.collections)
        query_type = 'find'

        attributes = self.get_attribute_types(collection)
        list_attributes = [key for key, value in attributes.items() if value == 'list']
        attr = random.choice(list_attributes)
        doc_len = self.db[collection].count_documents(
            {attr: {"$exists": True}}
        )
        random_index = random.randint(
            2, 
            doc_len
        ) - 1
        random_doc = self.db[collection].find(
            {attr: {"$exists": True}}
        ).skip(random_index).limit(1)
        values_subset = list(random_doc[0][attr])
        values_subset = random.sample(
            values_subset, 
            min(len(values_subset), random.randint(3,5))
        )

        query_params = {
            attr: {
                "$all": values_subset
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

    def template_find_array_in(self):
        collection = random.choice(self.collections)
        query_type = 'find'

        attributes = self.get_attribute_types(collection)
        list_attributes = [key for key, value in attributes.items() if value == 'list']
        attr = random.choice(list_attributes)
        doc_len = self.db[collection].count_documents(
            {attr: {"$exists": True}}
        )
        random_index = random.randint(
            2, 
            doc_len
        ) - 1
        random_doc = self.db[collection].find(
            {attr: {"$exists": True}}
        ).skip(random_index).limit(1)
        values_subset = list(random_doc[0][attr])
        values_subset = random.sample(
            values_subset, 
            min(len(values_subset), random.randint(3,5))
        )

        query_params = {
            attr: {
                "$in": values_subset
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

    # def template_find_elemmatch(self):
    #     collection = random.choice(self.collections)
    #     query_type = 'find'
    #     attributes = self.get_attribute_types(collection)
    #     dict_attributes = [key for key, value in attributes.items() if value == 'dict']
    #     attr = random.choice(dict_attributes)
    #     unique_values = self.db[collection].distinct(attr)
    #     unique_values = list(unique_values)
    #     value = random.choice(unique_values)
    #     print(value)
    #     nested_key1, nested_key2 = random.sample(list(value.keys()), 2)
    #     nested_value1, nested_value2 = value[nested_key1], value[nested_key2]
    #     query_params = {
    #         attr: {
    #             "$elemMatch": {
    #                 nested_key1: nested_value1,
    #                 nested_key2: nested_value2
    #             }
    #         }
    #     }
    #     query_projection = {
    #         attr: 1,
    #         '_id': 1
    #     }
    #     query_str = f"""db.{collection}.{query_type}({query_params},{query_projection})"""
    #     docs = self.db[collection].find(
    #         {
    #             attr: {
    #                 "$elemMatch": {
    #                     nested_key1: {"$eq": nested_value1},
    #                     nested_key2: {"$eq": nested_value2}
    #                 }
    #             }
    #         },
    #         {
    #             attr: 1,
    #             "_id": 1
    #         }
    #     )
    #     for doc in docs:
    #         print(doc)
    #     return {
    #         'query_type': query_type,
    #         'query_params': query_params,
    #         'collection': collection,
    #         'query_str': query_str,
    #         'query_projection': query_projection
    #     }

    def template_update_one_set(self):
        pass

    def template_update_one_unset(self):
        pass

    def template_update_many(self):
        pass

    def template_sort(self):
        pass

    def template_count_documents(self):
        pass

    def template_distinct(self):
        pass

    def template_match(self):
        pass

    def template_project(self):
        pass

    def template_unwind(self):
        pass

    def template_multiply(self):
        pass

    def template_lookup(self):
        pass