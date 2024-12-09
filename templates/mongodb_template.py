from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import random
from config.mongo_config import MONGO_URI
import json
import re

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
            elif query['query_type'] == "insertOne":
                # Return insert acknowledgment as string
                return {"acknowledged": result.acknowledged, "inserted_ids": getattr(result, "inserted_ids", [result.inserted_id])}
            elif query['query_type'] == "insertMany":
                # Return insert acknowledgment as string
                return {"acknowledged": result}
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

    def template_insert_one(self, params=None):
        collection, document, doc_str = None, None, None
        if params == None:
            collection = random.choice(self.collections)
            document = self.generate_sample_document()
            doc_str = json.dumps(document)
        else:
            collection = params['collection']
            doc_str = params['doc_str']
            document = json.loads(doc_str)
        query_type = 'insertOne'
        query_str = f"""db.{collection}.{query_type}({doc_str})"""
        query_desc = f"""Inserted one document: '{document}' in '{collection}' collection : """
        return {
            'query_type': query_type,
            'query_params': document,
            'collection': collection,
            'query_str': query_str,
            'query_desc': query_desc
        }

    def template_insert_many(self):
        documents = self.generate_sample_documents()
        collection = random.choice(self.collections)
        doc_str = json.dumps(documents)
        query_type = 'insertMany'
        query_str = f"""db.{collection}.{query_type}({doc_str})"""
        query_desc = f"""Inserted multiple documents: '{doc_str}' in '{collection}' collection : """
        return {
            'query_type': query_type,
            'query_params': documents,
            'collection': collection,
            'query_str': query_str,
            'query_desc': query_desc
        }
    
    def template_find_math_operations(self, collection=None, query_params=None):
        collection = random.choice(self.collections)
        query_type = 'find'
        print("Hi")

        attributes = self.get_attribute_types(collection)
        int_attributes = [key for key, value in attributes.items() if value == 'int']
        print(int_attributes)
        attr = random.choice(int_attributes)
        unique_values = self.db[collection].distinct(attr)
        unique_values = list(unique_values)
        value = random.choice(unique_values)
        op = random.choice(self.math_operations)
        print("Hi2")
        query_params = {
            attr: {
                op: value
            }
        }
        math_op_str = {
            #['$gte', '$gt', '$lte', '$lt', '$eq', '$ne']
            '$gte': 'greater than or equal to',
            '$gt': 'greater than',
            '$lte': 'less than or equal to',
            '$lt': 'less than',
            '$eq': 'equal to',
            '$ne': 'not equal to',
        }
        query_params_desc = str(attr)+' '+math_op_str[op]+' '+str(value)
        print("Hi3")
        if op=='$ne':
            query_params[attr]['$exists'] = True
        query_projection = {
            attr: 1,
            '_id': 1
        }
        print("Hi4")
        query_str = f"""db.{collection}.{query_type}({query_params},{query_projection})"""
        query_desc = f"""Found results of math operation {query_params_desc} on '{collection}' collection """            

        return {
            'query_type': query_type,
            'query_params': query_params,
            'collection': collection,
            'query_str': query_str,
            'query_projection': query_projection,
            'query_desc': query_desc
        }

    
    def template_find(self, params=None):
        collection = None
        if params == None:
            collection = random.choice(self.collections)
        else:
            collection = params['collection']
        query_type = 'find'
        query_params = ""
        query_str = f"""db.{collection}.{query_type}({query_params})"""
        query_desc = f"""Found the entries in '{collection}' collection : """
        return {
            'query_type': query_type,
            'query_params': query_params,
            'collection': collection,
            'query_str': query_str,
            'query_desc': query_desc,
            'message': "success"
        }

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

        query_desc = f"""Found results with conditions: '{query_params}' on '{collection}' collection : """

        return {
            'query_type': "find",
            'query_params': query_params,
            'collection': collection,
            'query_str': query_str,
            'query_projection': projection,
            'query_desc': query_desc,
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
        query_desc = f"""Display '{query_type}' results where '{attr}' attribute contains the word '{value}' : """
        return {
            'query_type': query_type,
            'query_params': query_params,
            'collection': collection,
            'query_str': query_str,
            'query_projection': query_projection,
            'query_desc': query_desc
        }
    
    def template_find_regex_2(self, params=None):
        query_type = 'find'
        if params== None:
            collection = random.choice(self.collections)
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
            else:
                value = value[-1]
        else:
            collection = params['collection']
            attr = params['attr']
            value = params['value']
            starts_with = params['starts_with']

        if starts_with == 1:
            query_params = {
                attr: {
                    "$regex": f"^{value}"
                }
            }
            query_params_str = {
                attr: f'/^{value}/'
            }
        else:
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
        if starts_with == 1:
            query_desc = f"""Display '{query_type}' results where value of '{attr}' starts with the word '{value}' : """
        else:
            query_desc = f"""Display '{query_type}' results where value of '{attr}' ends with the word '{value}' : """

        return {
            'query_type': query_type,
            'query_params': query_params,
            'collection': collection,
            'query_str': query_str,
            'query_projection': query_projection,
            'query_desc': query_desc
        }
    
    def template_find_and(self, params=None):
        query_type = 'find'
        if params == None:
            collection = random.choice(self.collections)
            attributes = self.get_attribute_types(collection)
            int_attributes = [key for key, value in attributes.items() if value == 'int']
            attr = random.choice(int_attributes)
            unique_values = self.db[collection].distinct(attr)
            unique_values = list(unique_values)
            value1, value2 = sorted(random.sample(unique_values, 2))
        else:
            collection = params['collection']
            attr = params['attr']
            value1 = params['value1']
            value2 = params['value2']

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
        query_desc = f"""Display '{query_type}' results where value of '{attr}' greater than equal to '{value1}' AND less than equal to '{value2}' : """

        return {
            'query_type': query_type,
            'query_params': query_params,
            'collection': collection,
            'query_str': query_str,
            'query_projection': query_projection,
            'query_desc': query_desc
        }
    
    def template_find_or(self, params=None):
        query_type = 'find'
        if params == None:
            collection = random.choice(self.collections)
            attributes = self.get_attribute_types(collection)
            int_attributes = [key for key, value in attributes.items() if value == 'int']
            attr = random.choice(int_attributes)
            unique_values = self.db[collection].distinct(attr)
            unique_values = list(unique_values)
            value1, value2 = sorted(random.sample(unique_values, 2))
        else:
            collection = params['collection']
            attr = params['attr']
            value1 = params['value1']
            value2 = params['value2']

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
        query_desc = f"""Display '{query_type}' results where value of '{attr}' equal to '{value1}' OR '{value2}' : """

        return {
            'query_type': query_type,
            'query_params': query_params,
            'collection': collection,
            'query_str': query_str,
            'query_projection': query_projection,
            'query_desc': query_desc
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
        query_desc = f"""Display '{query_type}' results where value of '{attr}' with key as '{nested_key}' is '{nested_value}' : """

        return {
            'query_type': query_type,
            'query_params': query_params,
            'collection': collection,
            'query_str': query_str,
            'query_projection': query_projection,
            'query_desc': query_desc
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
        query_desc = f"""Displays all the values of '{attr}' that are in '{values_subset}' : """
        return {
            'query_type': query_type,
            'query_params': query_params,
            'collection': collection,
            'query_str': query_str,
            'query_projection': query_projection,
            'query_desc': query_desc
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
        query_desc = f"""Displays the values of '{attr}' that are in '{values_subset}' : """

        return {
            'query_type': query_type,
            'query_params': query_params,
            'collection': collection,
            'query_str': query_str,
            'query_projection': query_projection,
            'query_desc': query_desc
        }

    def natural_lang_query_2(self, input):
        tokens = input.split(" ")
        if "display" in tokens and "all" in tokens:
            collections = self.collections
            for collection in collections:
                if collection in tokens:
                    params = {
                        "collection": collection
                    }
                    result = self.template_find(params=params)
                    print(result)
                    return result
                    break
        elif "insert" in tokens:
            for collection in self.collections:
                if collection in tokens:
                    matches = re.findall(r'\{[^{}]*\}', input)
                    doc_str = matches[0]
                    params = {
                        "collection": collection,
                        "doc_str": doc_str
                    }
                    result = self.template_insert_one(params=params)
                    print(result)
                    return result
                    break
        elif "starts" in tokens and "with" in tokens:
            for collection in self.collections:
                if collection in tokens:
                    attributes = self.get_attribute_types(collection)
                    for attr in attributes:
                        if attr in tokens:
                            value = tokens[-1]
                            params = {
                                "collection": collection,
                                "attr": attr,
                                "value": value,
                                "starts_with": 1
                            }
                            result = self.template_find_regex_2(params=params)
                            print(result)
                            return result
                            break
        elif "ends" in tokens and "with" in tokens:
            for collection in self.collections:
                if collection in tokens:
                    attributes = self.get_attribute_types(collection)
                    for attr in attributes:
                        if attr in tokens:
                            value = tokens[-1]
                            params = {
                                "collection": collection,
                                "attr": attr,
                                "value": value,
                                "starts_with": 0
                            }
                            result = self.template_find_regex_2(params=params)
                            print(result)
                            return result
                            break
        elif "and" in tokens and "greater" in tokens and "less" in tokens:
            for collection in self.collections:
                if collection in tokens:
                    attributes = self.get_attribute_types(collection)
                    for attr in attributes:
                        if attr in tokens:
                            matches = re.findall(r'greater than (\d+)', input)
                            value1 = int(matches[0])
                            matches = re.findall(r'less than (\d+)', input)
                            value2 = int(matches[0])
                            params = {
                                "collection": collection,
                                "attr": attr,
                                "value1": value1,
                                "value2": value2
                            }
                            result = self.template_find_and(params=params)
                            print(result)
                            return result
                            break
        elif "or" in tokens:
            for collection in self.collections:
                if collection in tokens:
                    attributes = self.get_attribute_types(collection)
                    for attr in attributes:
                        if attr in tokens:
                            matches = re.findall(r'(\d+) or (\d+)', input)
                            values = [(int(num1), int(num2)) for num1, num2 in matches]
                            value1, value2 = values[0]
                            params = {
                                "collection": collection,
                                "attr": attr,
                                "value1": value1,
                                "value2": value2
                            }
                            result = self.template_find_or(params=params)
                            print(result)
                            return result
                            break