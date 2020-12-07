import os
import sqlite3
from array import array
from sample_class import SampleClass


class Py2SQL:
    def __init__(self):
        self.filename = None
        self.connection = None
        self.cursor = None

    @staticmethod
    def __sqlite_type(obj):
        if type(obj) == int:
            columns = ['Value', ]
            types = ['INTEGER', ]
        elif type(obj) == float:
            columns = ['Value', ]
            types = ['REAL', ]
        elif type(obj) == array:
            columns = ['TypeCode', 'Value', ]
            types = ['TEXT', 'TEXT', ]
        elif type(obj) in (str, list, tuple, set, frozenset, dict):
            columns = ['Value', ]
            types = ['TEXT', ]
        else:  # object
            columns = [''.join(list(map(str.capitalize, attr.split('_')))) for attr in obj.__dict__]
            types = [Py2SQL.__sqlite_type(value)[1] for value in obj.__dict__.values()]

        return columns, types

    @staticmethod
    def __sqlite_repr(obj):
        if type(obj) in (set, frozenset, list, tuple):
            return (str(list(obj))[1:-1], )
        elif type(obj) == dict:
            return (str(obj)[1:-1], )
        elif type(obj) == array:
            return obj.typecode, str(list(obj))[1:-1]
        elif type(obj) in (int, float, str):
            return (obj, )
        else:  # object
            return tuple(obj.__dict__.values())

    def db_connect(self, db_filepath: str):
        """
        Connect to the database in given path

        :type db_filepath: str
        :param db_filepath: path to the database file
        :return: None
        """
        self.filename = db_filepath
        self.connection = sqlite3.connect(db_filepath)
        self.cursor = self.connection.cursor()

    def db_disconnect(self):
        self.connection.close()
        self.filename = None
        self.connection = None
        self.cursor = None

    def db_engine(self):
        """
        Retrieve database name and version

        :rtype: tuple
        :return: database name and version tuple
        """
        self.cursor.execute('SELECT sqlite_version();')
        version = self.cursor.fetchone()[0]
        name = self.db_name()
        return name, version

    def db_name(self):
        pass

    def db_size(self):
        """
        Retrieve connected database size in Mb

        :rtype: float
        :return: database size in Mb
        """
        return os.path.getsize(self.filename) / (1024 * 1024.0)

    def db_tables(self):
        pass

    def db_table_structure(self, table_name: str):
        """
        Retrieve ordered list of tuples of form (id, name, type) which describe given table's columns

        :type table_name: str
        :param table_name: name of the table to retrieve structure of
        :return: ordered list of tuples of form (id, name, type)
        """
        return list(map(lambda x: x[:3], self.cursor.execute('PRAGMA table_info(' + table_name + ');').fetchall()))

    def db_table_size(self, table_name: str):
        pass

    # Python -> SQLite

    def save_object(self, obj):
        """
        Save given object instance's representation into database or update it if it already exists

        :param obj: object instance to be saved
        :return: None
        """
        table_name = 'object_' + type(obj).__name__
        columns, types = Py2SQL.__sqlite_type(obj)
        id_column_name, id_column_type = 'ID', 'INTEGER'
        # print('''
        #                     CREATE TABLE {} (
        #                     {} {} PRIMARY KEY,
        #                     {}
        #                     );
        #                     '''.format(table_name,
        #                                id_column_name,
        #                                id_column_type,
        #                                ','.join(['{} {} NOT NULL'.format(columns[i], types[i])
        #                                          for i in range(len(types))])))
        self.cursor.execute('''
                            CREATE TABLE {} (
                            {} {} PRIMARY KEY,
                            {}
                            );
                            '''.format(table_name,
                                       id_column_name,
                                       id_column_type,
                                       ','.join(['{} {} NOT NULL'.format(columns[i], types[i])
                                                 for i in range(len(types))]))
                            )
        columns.insert(0, id_column_name)
        types.insert(0, id_column_type)

        values = (id(obj), *Py2SQL.__sqlite_repr(obj))

        # if hasattr(obj, '__dict__'):  # object
        #     map(self.save_object, tuple(obj.__dict__.values()))

        self.cursor.execute('''
        INSERT OR REPLACE INTO {} {}
        VALUES ({});
        '''.format(table_name, str(tuple(columns)), ('?,' * len(values))[:-1]), values)
        self.connection.commit()

    def save_class(self, cls):
        """
        Save given class instance's representation into database or update it if it already exists

        :param cls: class instance to be saved
        :return: None
        """
        pass

    def save_hierarchy(self, root_class):
        pass

    def delete_object(self, obj):
        """
        Delete given object instance's representation from database if it already existed

        :param obj: object instance to be saved
        :return: None
        """
        pass

    def delete_class(self, cls):
        """
        Delete given class instance's representation from database if it already existed

        :param cls: object instance to be saved
        :return: None
        """
        pass

    def delete_hierarchy(self, root_class):
        pass


if __name__ == '__main__':
    database_filepath = 'example.db'
    os.remove(database_filepath)

    py2sql = Py2SQL()
    py2sql.db_connect(database_filepath)
    showcase_table_name = 'object_int'

    py2sql.save_object(1)
    py2sql.save_object(1.1)
    py2sql.save_object('some str')
    py2sql.save_object([1, 2])
    py2sql.save_object((1, 2))
    py2sql.save_object({1, 2})
    py2sql.save_object(frozenset((1, 2)))
    py2sql.save_object({'key': 'str', 1: 'int', (1, 2, 3): 'tuple'})
    py2sql.save_object(array('i', [1, 2]))
    # py2sql.save_object(SampleClass())

    print('Engine:', py2sql.db_engine())
    print('Name:', py2sql.db_name())
    print('Size in Mb:', py2sql.db_size())
    print('Tables:', py2sql.db_tables())
    print('{} table structure:'.format(showcase_table_name), py2sql.db_table_structure(showcase_table_name))
    print('{} table size:'.format(showcase_table_name), py2sql.db_table_size(showcase_table_name))

    py2sql.db_disconnect()
