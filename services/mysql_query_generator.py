# import mysql.connector
# import random


# class MySQLTemplate:
#     def __init__(self, db_name):
#         """
#         Initialize MySQL connection and fetch metadata for the specified database.
#         """
#         self.connection = mysql.connector.connect(
#             host="localhost",  # Replace with your MySQL host
#             user="root",       # Replace with your MySQL username
#             password="password123",  # Replace with your MySQL password
#             database=db_name
#         )
#         self.db_name = db_name
#         self.cursor = self.connection.cursor(dictionary=True)
#         self.tables = self.get_tables()
#         self.foreign_keys = self.get_foreign_keys()
#         self.relational_operators = ["=", ">", "<", ">=", "<=", "<>"]
#         self.arithmetic_operations = ["AVG", "COUNT", "MAX", "MIN", "SUM"]
#         self.order_options = ["ASC", "DESC"]
#         self.join_options = ["INNER", "RIGHT", "LEFT", "FULL"]

#     def __del__(self):
#         """
#         Close the database connection when the object is deleted.
#         """
#         if self.cursor:
#             self.cursor.close()
#         if self.connection:
#             self.connection.close()

#     # Helper Functions

#     def get_tables(self):
#         """
#         Fetch all table names in the database.
#         """
#         query = "SHOW TABLES;"
#         self.cursor.execute(query)
#         return [row[f"Tables_in_{self.connection.database}"] for row in self.cursor.fetchall()]

#     def get_column_names(self, table):
#         """
#         Fetch all column names in a table.
#         """
#         query = f"SHOW COLUMNS FROM `{table}`;"
#         self.cursor.execute(query)
#         return [col["Field"] for col in self.cursor.fetchall()]

#     def get_column_values(self, table, column):
#         """
#         Fetch distinct values from a column in a table with error handling.
#         """
#         query = f"SELECT DISTINCT {column} FROM {table} LIMIT 10;"
#         try:
#             self.cursor.execute(query)
#             result = [row[column] for row in self.cursor.fetchall()]
#             return result
#         except Exception as e:
#             print(f"Error fetching values from {table}.{column}: {e}")
#             return []  # Return an empty list on failure


#     def get_column_types(self, table):
#         """
#         Fetch column names and their data types for a table.
#         """
#         query = f"SHOW COLUMNS FROM `{table}`;"
#         self.cursor.execute(query)
#         return {col["Field"]: col["Type"] for col in self.cursor.fetchall()}

#     # Query Templates

#     def template_select(self, limit=5):
#         """
#         Generate a simple SELECT query.
#         """
#         table = random.choice(self.tables)
#         query = f"SELECT * FROM `{table}` LIMIT {limit};"
#         return {"query": query, "message": "success"}

#     def template_distinct(self):
#         """
#         Generate a DISTINCT query.
#         """
#         table = random.choice(self.tables)
#         column = random.choice(self.get_column_names(table))
#         query = f"SELECT DISTINCT `{column}` FROM `{table}`;"
#         return {"query": query, "message": "success"}

#     def template_where(self):
#         """
#         Generate a WHERE query.
#         """
#         table = random.choice(self.tables)
#         column = random.choice(self.get_column_names(table))
#         value = random.choice(self.get_column_values(table, column))
#         query = f"SELECT * FROM `{table}` WHERE `{column}` = '{value}';"
#         return {"query": query, "message": "success"}

#     def template_order_by(self):
#         """
#         Generate an ORDER BY query.
#         """
#         table = random.choice(self.tables)
#         column = random.choice(self.get_column_names(table))
#         order = random.choice(self.order_options)
#         query = f"SELECT * FROM `{table}` ORDER BY `{column}` {order};"
#         return {"query": query, "message": "success"}

#     def template_group_by(self):
#         """
#         Generate a GROUP BY query with an aggregate function.
#         """
#         for _ in range(10):  # Attempt 10 times to find a suitable table
#             table = random.choice(self.tables)
#             columns = self.get_column_names(table)
#             if len(columns) > 1:
#                 group_column = random.choice(columns)
#                 agg_column = random.choice(columns)
#                 agg_func = random.choice(self.arithmetic_operations)
#                 query = f"SELECT `{group_column}`, {agg_func}(`{agg_column}`) FROM `{table}` GROUP BY `{group_column}`;"
#                 return {"query": query, "message": "success"}
#         return {"query": "", "result": [], "message": "Unable to generate GROUP BY query"}

#     def template_join(self):
#         """
#         Generate a JOIN query.
#         """
#         if not self.foreign_keys:
#             return {"query": "", "result": [], "message": "No foreign keys available for JOIN query"}

#         foreign_key = random.choice(self.foreign_keys)
#         join_type = random.choice(self.join_options)
#         query = (
#             f"SELECT * FROM `{foreign_key['table_name']}` {join_type} JOIN `{foreign_key['referenced_table']}` "
#             f"ON `{foreign_key['table_name']}`.`{foreign_key['column_name']}` = "
#             f"`{foreign_key['referenced_table']}`.`{foreign_key['referenced_column']}`;"
#         )
#         return {"query": query, "message": "success"}

#     def template_in(self):
#         """
#         Generate an IN query.
#         """
#         table = random.choice(self.tables)
#         column = random.choice(self.get_column_names(table))
#         values = self.get_column_values(table, column)
#         if len(values) < 2:
#             return {"query": "", "result": [], "message": "Not enough values for IN query"}
#         value_list = ", ".join([f"'{v}'" for v in random.sample(values, min(len(values), 5))])
#         query = f"SELECT * FROM `{table}` WHERE `{column}` IN ({value_list});"
#         return {"query": query, "message": "success"}

#     def get_column_num_names(self, table):
#         """
#         Fetch numeric column names for the given table with enhanced error handling.
#         """
#         try:
#             query = f"""
#             SELECT COLUMN_NAME
#             FROM information_schema.COLUMNS
#             WHERE TABLE_NAME = '{table}'
#             AND TABLE_SCHEMA = '{self.db_name}'
#             AND DATA_TYPE IN ('int', 'decimal', 'float', 'double', 'numeric', 'bigint', 'smallint', 'tinyint');
#             """
#             self.cursor.execute(query)
#             columns = [row["COLUMN_NAME"] for row in self.cursor.fetchall()]
            
#             # Log the fetched columns for debugging
#             print(f"Numeric columns for table {table}: {columns}")
            
#             return columns
#         except Exception as e:
#             print(f"Error fetching numeric columns for table {table}: {e}")
#             return []  # Return an empty list on failure

#     def template_between(self, threshold=10):
#         """
#         Generate a BETWEEN query with enhanced logging and error handling.
#         """
#         for _ in range(threshold):  # Retry up to 'threshold' times
#             table = random.choice(self.tables)
#             numeric_columns = self.get_column_num_names(table)

#             # Log numeric columns found
#             print(f"Numeric columns in {table}: {numeric_columns}")
            
#             if numeric_columns:
#                 column = random.choice(numeric_columns)
#                 values = self.get_column_values(table, column)
                
#                 # Log distinct values fetched
#                 print(f"Values in {table}.{column}: {values}")
                
#                 if len(values) >= 2:  # Ensure at least 2 distinct values are available
#                     try:
#                         start, end = sorted(random.sample(values, 2))
#                         query = f"SELECT * FROM {table} WHERE {column} BETWEEN {start} AND {end};"
#                         return {"query": query, "message": "success"}
#                     except Exception as e:
#                         print(f"Error sampling values: {e}")
#                         continue  # Retry with the next iteration
#                 else:
#                     print(f"Not enough distinct values in {table}.{column} for BETWEEN query.")
#             else:
#                 print(f"No numeric columns found in table {table}.")

#         return {"query": "", "message": "Unable to generate a valid BETWEEN query"}

    
#     def template_having(self, threshold=10):
#         for _ in range(threshold):
#             table = random.choice(self.tables)
#             columns_str = self.get_column_str_names(table)
#             columns_num = self.get_column_num_names(table)
#             if len(columns_str) > 0 and len(columns_num) > 0:
#                 column_str = random.choice(columns_str)
#                 column_num = random.choice(columns_num)
#                 arithm_op = random.choice(self.arithmetic_oerations)
#                 rel_op = random.choice(self.relational_operators)
#                 values = self.get_column_values(table, column_num)
#                 if not values:
#                     continue
#                 value = random.choice(values)
#                 query = """SELECT {}, {}({}) FROM {} GROUP BY {} HAVING {}({}) {} {}""".format(
#                     column_str, arithm_op, column_num, table, column_str,
#                     arithm_op, column_num, rel_op, value)
#                 return self.get_query_result(query)
#         return {
#             "query": "",
#             "result": [],
#             "message": "Unable to generate a HAVING query."
#         }

#     def execute_query(self, query):
#         """
#         Execute a SQL query and return the result.
#         """
#         # Add LIMIT 20 if not present in the query
#         if "limit" not in query.lower():
#             query = f"{query.rstrip(';')} LIMIT 20;"
        
#         try:
#             self.cursor.execute(query)
#             result = self.cursor.fetchall()
#             return result
#         except Exception as e:
#             print(f"Error executing query: {e}")
#             raise
#         # print(query)
#         # return {"query": query, "message": "success"}

#     def get_foreign_keys(self):
#         """
#         Fetch foreign keys in the database.
#         """
#         query = """
#         SELECT
#             kcu.TABLE_NAME AS table_name,
#             kcu.COLUMN_NAME AS column_name,
#             kcu.REFERENCED_TABLE_NAME AS referenced_table,
#             kcu.REFERENCED_COLUMN_NAME AS referenced_column
#         FROM
#             information_schema.KEY_COLUMN_USAGE AS kcu
#         WHERE
#             kcu.TABLE_SCHEMA = %s
#             AND kcu.REFERENCED_TABLE_NAME IS NOT NULL;
#         """
#         self.cursor.execute(query, (self.connection.database,))
#         return [
#             {
#                 "table_name": row["table_name"],
#                 "column_name": row["column_name"],
#                 "referenced_table": row["referenced_table"],
#                 "referenced_column": row["referenced_column"],
#             }
#             for row in self.cursor.fetchall()
#         ]

import mysql.connector
import random

class MySQLTemplate:
    def __init__(self, db_name):
        """
        Initialize MySQL connection and fetch metadata for the specified database.
        """
        self.connection = mysql.connector.connect(
            host="localhost",  # Replace with your MySQL host
            user="root",       # Replace with your MySQL username
            password="mysql",  # Replace with your MySQL password
            database=db_name
        )
        self.db_name = db_name
        self.cursor = self.connection.cursor(dictionary=True)
        self.tables = self.get_tables()
        self.foreign_keys = self.get_foreign_keys()
        self.relational_operators = ["=", ">", "<", ">=", "<=", "<>"]
        self.arithmetic_operations = ["AVG", "COUNT", "MAX", "MIN", "SUM"]
        self.order_options = ["ASC", "DESC"]
        self.join_options = ["INNER", "RIGHT", "LEFT", "FULL"]

    def __del__(self):
        """
        Close the database connection when the object is deleted.
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    # Helper Functions

    def get_tables(self):
        """
        Fetch all table names in the database.
        """
        query = "SHOW TABLES;"
        self.cursor.execute(query)
        return [row[f"Tables_in_{self.connection.database}"] for row in self.cursor.fetchall()]

    def get_column_names(self, table):
        """
        Fetch all column names in a table.
        """
        query = f"SHOW COLUMNS FROM `{table}`;"
        self.cursor.execute(query)
        return [col["Field"] for col in self.cursor.fetchall()]

    def get_column_values(self, table, column):
        """
        Fetch distinct values from a column in a table with error handling.
        """
        query = f"SELECT DISTINCT `{column}` FROM `{table}` LIMIT 10;"
        try:
            self.cursor.execute(query)
            result = [row[column] for row in self.cursor.fetchall()]
            return result
        except Exception as e:
            print(f"Error fetching values from {table}.{column}: {e}")
            return []  # Return an empty list on failure

    def get_column_types(self, table):
        """
        Fetch column names and their data types for a table.
        """
        query = f"SHOW COLUMNS FROM `{table}`;"
        self.cursor.execute(query)
        return {col["Field"]: col["Type"] for col in self.cursor.fetchall()}

    # Query Templates

    def template_select(self, limit=5, params=None):
        """
        Generate a simple SELECT query.
        """
        table = None
        if params == None:
            table = random.choice(self.tables)
        else:
            table = params['table']
        query = f"SELECT * FROM `{table}` LIMIT {limit};"
        desc = f"Display rows of table '{table}'"
        return {"query": query, "desc": desc, "message": "success"}

    def template_distinct(self, params=None):
        """
        Generate a DISTINCT query.
        """
        table, column = None, None
        if params == None:
            table = random.choice(self.tables)
            column = random.choice(self.get_column_names(table))
        else:
            table = params['table']
            column = params['column']
        query = f"SELECT DISTINCT `{column}` FROM `{table}`;"
        desc = f"Display distinct values of column '{column}' the table '{table}'"
        return {"query": query, "desc": desc, "message": "success"}

    def template_where(self, params=None):
        """
        Generate a WHERE query.
        """
        table, column, value = None, None, None
        if params == None:
            table = random.choice(self.tables)
            column = random.choice(self.get_column_names(table))
            value = random.choice(self.get_column_values(table, column))
        else:
            table = params['table']
            column = params['column']
            value = params['value']
        query = f"SELECT * FROM `{table}` WHERE `{column}` = '{value}';"
        desc = f"Display those records of table '{table}' whose '{column}' is '{value}'"
        return {"query": query, "desc": desc, "message": "success"}

    def template_order_by(self, params=None):
        """
        Generate an ORDER BY query.
        """
        table, column, order = None, None, None
        if params == None:
            table = random.choice(self.tables)
            column = random.choice(self.get_column_names(table))
            order = random.choice(self.order_options)
        else:
            table = params['table']
            column = params['column']
            order = params['order']
        query = f"SELECT * FROM `{table}` ORDER BY `{column}` {order};"
        desc = f"Display the records of table '{table}',  the records are sorted by '{column}'"
        return {"query": query, "desc": desc, "message": "success"}

    def template_group_by(self):
        """
        Generate a GROUP BY query with an aggregate function.
        """
        for _ in range(10):  # Attempt 10 times to find a suitable table
            table = random.choice(self.tables)
            columns = self.get_column_names(table)
            if len(columns) > 1:
                group_column = random.choice(columns)
                agg_column = random.choice(columns)
                agg_func = random.choice(self.arithmetic_operations)
                query = f"SELECT `{group_column}`, {agg_func}(`{agg_column}`) FROM `{table}` GROUP BY `{group_column}`;"
                return {"query": query, "message": "success"}
        return {"query": "", "result": [], "message": "Unable to generate GROUP BY query"}

    def template_join(self):
        """
        Generate a JOIN query.
        """
        if not self.foreign_keys:
            return {"query": "", "result": [], "message": "No foreign keys available for JOIN query"}

        foreign_key = random.choice(self.foreign_keys)
        join_type = random.choice(self.join_options)
        query = (
            f"SELECT * FROM `{foreign_key['table_name']}` {join_type} JOIN `{foreign_key['referenced_table']}` "
            f"ON `{foreign_key['table_name']}`.`{foreign_key['column_name']}` = "
            f"`{foreign_key['referenced_table']}`.`{foreign_key['referenced_column']}`;"
        )
        return {"query": query, "message": "success"}

    def template_in(self):
        """
        Generate an IN query.
        """
        table = random.choice(self.tables)
        column = random.choice(self.get_column_names(table))
        values = self.get_column_values(table, column)
        if len(values) < 2:
            return {"query": "", "result": [], "message": "Not enough values for IN query"}
        value_list = ", ".join([f"'{v}'" for v in random.sample(values, min(len(values), 5))])
        query = f"SELECT * FROM `{table}` WHERE `{column}` IN ({value_list});"
        return {"query": query, "message": "success"}

    def get_column_num_names(self, table):
        """
        Fetch numeric column names for the given table with enhanced error handling.
        """
        try:
            query = f"""
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_NAME = '{table}'
            AND TABLE_SCHEMA = '{self.db_name}'
            AND DATA_TYPE IN ('int', 'decimal', 'float', 'double', 'numeric', 'bigint', 'smallint', 'tinyint');
            """
            self.cursor.execute(query)
            columns = [row["COLUMN_NAME"] for row in self.cursor.fetchall()]
            return columns
        except Exception as e:
            print(f"Error fetching numeric columns for table {table}: {e}")
            return []  # Return an empty list on failure

    def get_column_str_names(self, table):
        """
        Fetch string column names for the given table.
        """
        try:
            query = f"""
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_NAME = '{table}'
            AND TABLE_SCHEMA = '{self.db_name}'
            AND DATA_TYPE IN ('varchar', 'char', 'text', 'longtext', 'mediumtext', 'tinytext');
            """
            self.cursor.execute(query)
            columns = [row["COLUMN_NAME"] for row in self.cursor.fetchall()]
            return columns
        except Exception as e:
            print(f"Error fetching string columns for table {table}: {e}")
            return []  # Return an empty list on failure

    def template_between(self, threshold=10):
        """
        Generate a BETWEEN query with enhanced logging and error handling.
        """
        for _ in range(threshold):  # Retry up to 'threshold' times
            table = random.choice(self.tables)
            numeric_columns = self.get_column_num_names(table)
            if numeric_columns:
                column = random.choice(numeric_columns)
                values = self.get_column_values(table, column)
                if len(values) >= 2:
                    try:
                        start, end = sorted(random.sample(values, 2))
                        query = f"SELECT * FROM `{table}` WHERE `{column}` BETWEEN {start} AND {end};"
                        return {"query": query, "message": "success"}
                    except Exception as e:
                        print(f"Error sampling values: {e}")
                        continue
        return {"query": "", "message": "Unable to generate a valid BETWEEN query"}

    def template_having(self, threshold=10):
        for _ in range(threshold):
            table = random.choice(self.tables)
            columns_str = self.get_column_str_names(table)
            columns_num = self.get_column_num_names(table)
            if len(columns_str) > 0 and len(columns_num) > 0:
                column_str = random.choice(columns_str)
                column_num = random.choice(columns_num)
                arithm_op = random.choice(self.arithmetic_operations)
                rel_op = random.choice(self.relational_operators)
                values = self.get_column_values(table, column_num)
                if not values:
                    continue
                value = random.choice(values)
                query = f"SELECT `{column_str}`, {arithm_op}(`{column_num}`) FROM `{table}` GROUP BY `{column_str}` HAVING {arithm_op}(`{column_num}`) {rel_op} {value};"
                return {"query": query, "message": "success"}
        return {"query": "", "message": "Unable to generate a HAVING query."}

    def execute_query(self, query):
        """
        Execute a SQL query and return the result.
        """
        # Add LIMIT 20 if not present in the query
        if "limit" not in query.lower():
            query = f"{query.rstrip(';')} LIMIT 20;"
        
        try:
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            return result
        except Exception as e:
            print(f"Error executing query: {e}")
            raise

    def get_foreign_keys(self):
        """
        Fetch foreign keys in the database.
        """
        query = """
        SELECT
            kcu.TABLE_NAME AS table_name,
            kcu.COLUMN_NAME AS column_name,
            kcu.REFERENCED_TABLE_NAME AS referenced_table,
            kcu.REFERENCED_COLUMN_NAME AS referenced_column
        FROM
            information_schema.KEY_COLUMN_USAGE AS kcu
        WHERE
            kcu.TABLE_SCHEMA = %s
            AND kcu.REFERENCED_TABLE_NAME IS NOT NULL;
        """
        self.cursor.execute(query, (self.connection.database,))
        return [
            {
                "table_name": row["table_name"],
                "column_name": row["column_name"],
                "referenced_table": row["referenced_table"],
                "referenced_column": row["referenced_column"],
            }
            for row in self.cursor.fetchall()
        ]