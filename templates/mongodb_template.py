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
    
    def template_find_math_operations(self):
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
        if op=='$ne':
            query_params[attr]['$exists'] = True
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