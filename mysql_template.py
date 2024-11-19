import mysql.connector
import random
from settings import *

class MySQLTemplate:
    def __init__(self, db_name):
        self.connection = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=db_name  # Dynamic database
        )
        self.cursor = self.connection.cursor()
        self.tables = self.get_tables()
        self.foreign_keys = self.get_foreign_keys()
        self.relational_operators = ["=", ">", "<", ">=", "<=", "<>"]
        self.arithmetic_oerations = ["AVG", "COUNT", "MAX", "MIN", "SUM"]
        self.order_options = ["ASC", "DESC"]
        self.join_options = ["INNER", "RIGHT", "LEFT", "FULL"]
    
    def __del__(self):
        self.cursor.close()
        self.connection.close()

    # Helper
    
    def get_tables(self):
        query = """SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{}'""".format(DATABASE)
        self.cursor.execute(query)
        tables = [row[0] for row in self.cursor.fetchall()]
        return tables
    
    def get_column_names(self, table):
        query = """SELECT COLUMN_NAME
            FROM information_schema.columns
            WHERE table_name = '{}'
            AND table_schema = '{}'""".format(
                table, DATABASE)
        self.cursor.execute(query)
        columns = [row[0] for row in self.cursor.fetchall()]
        return columns
    
    def get_column_str_names(self, table):
        query = """SELECT COLUMN_NAME
            FROM information_schema.columns
            WHERE table_name = '{}'
            AND table_schema = '{}'
            AND DATA_TYPE IN ('char', 'varchar', 'text', 'tinytext', 'mediumtext', 'longtext')""".format(
            table, DATABASE)
        self.cursor.execute(query)
        columns = [row[0] for row in self.cursor.fetchall()]
        return columns
    
    def get_column_num_names(self, table):
        query = """SELECT COLUMN_NAME
            FROM information_schema.columns
            WHERE table_name = '{}'
            AND table_schema = '{}'
            AND DATA_TYPE IN ('int', 'decimal', 'float', 'double', 'numeric', 'bigint', 'smallint', 'tinyint')""".format(
            table, DATABASE)
        self.cursor.execute(query)
        columns = [row[0] for row in self.cursor.fetchall()]
        return columns

    def get_column_values(self, table, column):
        query = """SELECT distinct {}
        FROM {}""".format(column, table)
        self.cursor.execute(query)
        values = [row[0] for row in self.cursor.fetchall()]
        return values
    
    def get_query_result(self, query):
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        return {
            'query': query,
            'result': result,
            'message': 'success'
        }
     
    def get_foreign_keys(self):
        query = """
        SELECT
            kcu.TABLE_NAME AS table_name,
            kcu.COLUMN_NAME AS column_name,
            kcu.CONSTRAINT_NAME AS foreign_key_name,
            kcu.REFERENCED_TABLE_NAME AS referenced_table,
            kcu.REFERENCED_COLUMN_NAME AS referenced_column
        FROM
            information_schema.KEY_COLUMN_USAGE kcu
        JOIN
            information_schema.REFERENTIAL_CONSTRAINTS rc
            ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
            AND kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
        WHERE
            kcu.TABLE_SCHEMA = '{}'
            AND kcu.REFERENCED_TABLE_NAME IS NOT NULL;
        """.format(DATABASE)
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        foreign_keys = [
            {
                'table_name': row[0],
                'column_name': row[1],
                'foreign_key_name': row[2],
                'referenced_table': row[3],
                'referenced_column': row[4]
            }
            for row in result
        ]
        return foreign_keys
    
    # Templates
    
    def template_select(self, limit=5):
        query = """SELECT * from {} limit {}""".format(
            random.choice(self.tables), limit)
        return self.get_query_result(query)
    
    def template_limit(self):
        query = """SELECT * from {} limit {}""".format(
            random.choice(self.tables), 
            random.randint(1,6))
        return self.get_query_result(query)
    
    def template_distinct(self, limit=5):
        table = random.choice(self.tables)
        columns = self.get_column_names(table)
        column = random.choice(columns)
        query = """SELECT distinct {} FROM {}""".format(
            column, table)
        return self.get_query_result(query)
    
    def template_where(self):
        table = random.choice(self.tables)
        columns = self.get_column_names(table)
        column = random.choice(columns)
        values = self.get_column_values(table, column)
        value = random.choice(values)
        #rel_op = random.choice(self.relational_operators)
        rel_op = "="
        query = """SELECT * FROM {} WHERE {} {} '{}'""".format(
            table, column, rel_op, value)
        return self.get_query_result(query)
    
    def template_order_by(self):
        table = random.choice(self.tables)
        columns = self.get_column_names(table)
        column = random.choice(columns)
        query = """SELECT * FROM {} ORDER BY {} {}""".format(
            table, column, random.choice(self.order_options))
        return self.get_query_result(query)

    def template_group_by(self, threshold=10):
        for iter in range(threshold):
            table = random.choice(self.tables)
            columns_str = self.get_column_str_names(table)
            columns_num = self.get_column_num_names(table)
            if (len(columns_str)>0 and len(columns_num)>0):
                column_num = random.choice(columns_num)
                column_str = random.choice(columns_str)
                arithm_op = random.choice(self.arithmetic_oerations)
                query = """SELECT {}, {}({}) FROM {} GROUP BY {}""".format(
                    column_str, arithm_op, column_num, table, column_str)
                return self.get_query_result(query)
        return {
            'query': "",
            'result': [],
            'message': "Not able to generate Group By Construct Query"
        }
    
    def template_join(self, join=''):
        foreign_key = random.choice(self.foreign_keys)
        join = random.choice(self.join_options) if join == '' else join
        query = """SELECT * FROM {} {} JOIN {} ON {}.{} = {}.{}""".format(
            foreign_key['table_name'],
            join,
            foreign_key['referenced_table'],
            foreign_key['table_name'],
            foreign_key['column_name'],
            foreign_key['referenced_table'],
            foreign_key['referenced_column'],
        )
        return self.get_query_result(query)

    def template_in(self, threshold=10):
        for iter in range(threshold):
            table = random.choice(self.tables)
            columns_str = self.get_column_str_names(table)
            if (len(columns_str)>0):
                column_str = random.choice(columns_str)
                values = self.get_column_values(table, column_str)
                k = min(len(values), random.randint(2, 5))
                samples = str(tuple(random.sample(values, k)))

                query = """SELECT * FROM {} WHERE {} IN {}""".format(
                    table, column_str, samples)
                return self.get_query_result(query)
        return {
            'query': "",
            'result': [],
            'message': "Not able to generate In Construct Query"
        }

    def template_between(self, threshold=10):
        for iter in range(threshold):
            table = random.choice(self.tables)
            columns_num = self.get_column_num_names(table)
            if (len(columns_num)>0):
                column_num = random.choice(columns_num)
                values = self.get_column_values(table, column_num)
                samples = random.sample(values, 2)
                samples.sort()
                query = """SELECT * FROM {} WHERE {} BETWEEN {} AND {}""".format(
                    table, column_num, samples[0], samples[1])
                return self.get_query_result(query)
        return {
            'query': "",
            'result': [],
            'message': "Not able to generate In Construct Query"
        }

    def template_having(self, threshold=10):
        for iter in range(threshold):
            table = random.choice(self.tables)
            columns_str = self.get_column_str_names(table)
            columns_num = self.get_column_num_names(table)
            if (len(columns_str)>0 and len(columns_num)>0):
                column_num = random.choice(columns_num)
                column_str = random.choice(columns_str)
                arithm_op = random.choice(self.arithmetic_oerations)
                rel_op = random.choice(self.relational_operators)
                values = self.get_column_values(table, column_num)
                value = random.choice(values)
                query = """SELECT {}, {}({}) FROM {} GROUP BY {} HAVING {}({}) {} {}""".format(
                    column_str, arithm_op, column_num, table, column_str,
                    arithm_op, column_num, rel_op, value)
                return self.get_query_result(query)
        return {
            'query': "",
            'result': [],
            'message': "Not able to generate Group By Construct Query"
        }