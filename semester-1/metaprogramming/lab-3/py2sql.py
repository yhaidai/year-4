"""
Module implements simple ORM for SQLite.

Module excludes using many-to-many and one-to-many relationships.
Trying to save the same object (update) with another aggregated object
will rewrite old object!
"""

import os
import sqlite3
from array import array
from inspect import *

from util import *
from demo_classes import SampleClass
from demo_classes import *


class Py2SQL:
    def __init__(self):
        self.filename = None
        self.connection = None
        self.cursor = None

    @staticmethod
    def __is_of_primitive_type(obj) -> bool:
        """
        Check whether given object is of primitive type i.e. is represented by a single field in SQLite database, thus
        can be embedded into 'composite' objects

        :param obj: object to be type-checked
        :rtype: bool
        :return: True if object is of primitive type, False otherwise
        """
        return type(obj) in (int, float, str, dict, tuple, list, set, frozenset)

    @staticmethod
    def __get_object_table_name(obj) -> str:
        """
        Build name of the table which should store objects of the same type as given one

        :param obj: object to build respective table name from
        :rtype: str
        :return: name of table to store object in
        """
        return 'object_' + type(obj).__name__

    @staticmethod
    def __sqlite_type(obj) -> tuple:
        """
        Retrieve column names and types of SQLite table which should store objects of the same type as given one

        int, float and str are represented as INTEGER, REAL and TEXT fields respectively
        set, frozenset, list, tuple, dict collections are stored in TEXT field as comma separated list of their elements
        array is represented as two TEXT fields: first containing its typecode and second containing its elements
        object is represented as tuple of its attributes whereas each attribute of primitive type is stored as
        described above meanwhile each composite attribute is represented by foreign key INTEGER field containing id of
        the referenced object

        :param obj: object to be stored in SQLite table
        :rtype: tuple
        :return: tuple of two lists containing column names and types respectively, list containing column types stores
        two-element tuples of form (sqlite_type: str, foreign_key_reference: str) where foreign_key_reference being None
        means absence of the reference while if some other table is to be referenced it should be equal to the name
        of the respective table
        """
        if type(obj) == int:
            columns = ['Value', ]
            types = [('INTEGER', None), ]
        elif type(obj) == float:
            columns = ['Value', ]
            types = [('REAL', None), ]
        elif type(obj) == array:
            columns = ['TypeCode', 'Value', ]
            types = [('TEXT', None), ('TEXT', None), ]
        elif type(obj) in (str, list, tuple, set, frozenset, dict):
            columns = ['Value', ]
            types = [('TEXT', None), ]
        else:  # object
            columns = [''.join(list(map(str.capitalize, attr.split('_')))) for attr in obj.__dict__]
            types = [(Py2SQL.__sqlite_type(value)[1], None) if Py2SQL.__is_of_primitive_type(value)
                     else ('INTEGER', Py2SQL.__get_object_table_name(value)) for value in obj.__dict__.values()]

        return columns, types

    @staticmethod
    def __sqlite_repr(obj) -> tuple:
        """
        Retrieve SQLite representation of given object

        int, float and str are represented as INTEGER, REAL and TEXT fields respectively
        set, frozenset, list, tuple, dict collections are stored in TEXT field as comma separated list of their elements
        array is represented as two TEXT fields: first containing its typecode and second containing its elements
        object is represented as tuple of its attributes whereas each attribute of primitive type is stored as
        described above meanwhile each composite attribute is represented by foreign key INTEGER field containing id of
        the referenced object

        :param obj: object to be represented in SQLite database
        :rtype: tuple
        :return: tuple of values(presumably object's attributes) to be stored in the respective database table
        """
        if type(obj) in (set, frozenset, list, tuple):
            return (str(list(obj))[1:-1],)
        elif type(obj) == dict:
            return (str(obj)[1:-1],)
        elif type(obj) == array:
            return obj.typecode, str(list(obj))[1:-1]
        elif type(obj) in (int, float, str):
            return (obj,)
        else:  # object
            return tuple(obj.__dict__.values())

    def db_connect(self, db_filepath: str) -> None:
        """
        Connect to the database in given path

        :type db_filepath: str
        :param db_filepath: path to the database file
        :return: None
        """
        self.filename = db_filepath
        self.connection = sqlite3.connect(db_filepath)
        self.cursor = self.connection.cursor()

    def db_disconnect(self) -> None:
        """
        Disconnect from the current database

        :return: None
        """
        self.connection.close()
        self.filename = None
        self.connection = None
        self.cursor = None

    def db_engine(self) -> tuple:
        """
        Retrieve database name and version

        :rtype: tuple
        :return: database name and version tuple
        """
        self.cursor.execute('SELECT sqlite_version();')
        version = self.cursor.fetchone()[0]
        name = self.db_name()
        return name, version

    def db_name(self) -> str:
        query = "PRAGMA database_list;"
        self.cursor.execute(query)
        db_info = self.cursor.fetchone()

        if db_info:
            return db_info[1]
        return ""

    def db_size(self) -> float:
        """
        Retrieve connected database size in Mb

        :rtype: float
        :return: database size in Mb
        """
        return os.path.getsize(self.filename) / (1024 * 1024.0)

    def db_tables(self):
        """
        Retrieve all the tables names present in database.

        :return: list of database tables names
        """
        query = "SELECT tbl_name FROM sqlite_master;"
        self.cursor.execute(query)
        tables_info = self.cursor.fetchall()
        return list(map(lambda t: t[0], list(tables_info)))


    def db_table_structure(self, table_name: str) -> list:
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

    def save_object(self, obj) -> None:
        """
        Save given object instance's representation into database or update it if it already exists

        :param obj: object instance to be saved
        :return: None
        """
        table_name = Py2SQL.__get_object_table_name(obj)
        columns, types = Py2SQL.__sqlite_type(obj)
        id_column_name, id_column_type = 'ID', 'INTEGER'
        try:
            self.cursor.execute('''
                                CREATE TABLE {} (
                                {} {} PRIMARY KEY,
                                {}
                                );
                                '''.format(table_name,
                                           id_column_name,
                                           id_column_type,
                                           ','.join(
                                               ['{} {} NOT NULL'.format(columns[i], types[i][0]) if types[i][1] is None
                                                else '{} {} REFERENCES {}(ID)'.format(columns[i], *types[i])
                                                for i in range(len(types))]))
                                )
        except sqlite3.OperationalError:  # table already exists
            pass
        columns.insert(0, id_column_name)

        if hasattr(obj, '__dict__'):  # object
            values = [id(obj)]
            for value in obj.__dict__.values():
                if not Py2SQL.__is_of_primitive_type(value):
                    self.save_object(value)
                    values.append(id(value))  # save foreign key for composite object
                else:
                    values.append(Py2SQL.__sqlite_repr(value)[0])
        else:
            values = (id(obj), *Py2SQL.__sqlite_repr(obj))

        self.cursor.execute('''
        INSERT OR REPLACE INTO {} {}
        VALUES ({});
        '''.format(table_name, str(tuple(columns)), ('?,' * len(values))[:-1]), values)
        self.connection.commit()

    @staticmethod
    def __is_magic_attr(attr_name: str) -> bool:
        """
        Defines is given attribute name is built-in magic attribute name

        :param attr_name:
        :return: bool
        """
        return attr_name.startswith("__") and attr_name.endswith("__")

    @staticmethod
    def __is_primitive_type(cls_obj):
        """
        Checks if input class object belongs to primitive built-in types
        :param cls_obj:
        :return: bool
        """

        return cls_obj in (int, float, str, dict, tuple, list, set, frozenset)

    @staticmethod
    def __sqlite_column_from_primitive(attr_class, attr_name="") -> tuple:
        """
        Implementing python on sqlite types mapping
        Author: Yehor

        :param obj:
        :return: (column sqlite type, column name)
        """
        if not Py2SQL.__is_primitive_type(attr_class):
            raise ValueError("Primitive type expected!")

        # TODO
        col_name = ""

        if attr_class == int:
            return "INTEGER", col_name
        elif attr_class == float:
            return 'REAL', col_name
        elif attr_class == array:
            return "TEXT", col_name
        elif attr_class in (str, list, tuple, set, frozenset, dict):
            return 'TEXT', col_name

    @staticmethod
    def __get_column_name(cls, attr_name="") -> str:
        if attr_name == "":
            return cls.__name__
        else:
            return attr_name

    @staticmethod
    def __get_class_table_name(cls_obj):
        """Defines database table name for class representation

            :return str
        """

        prefix = cls_obj.__module__.replace(".", "_") + "_"
        if Py2SQL.__is_of_primitive_type(cls_obj):
            return prefix + cls_obj.__name__
        return prefix + cls_obj.__name__

    def __table_exists(self, table_name):
        """
        Checks if table with table name exists in database
        :param table_name: table name
        :return: bool, exists or not
        """

        for tbl_name in self.db_tables():
            if tbl_name == table_name:
                return True
        return False

    def __create_or_update_table(self, cls):
        """
        Consider cls as primitive type or as class with primitive attributes
        :param cls: primitive type (int, str, ...,) or class with primitive attributes
        :return: table name, id column name
        """
        table_name = self.__get_class_table_name(cls)
        if (self.__is_primitive_type(cls)):
            col_name = Py2SQL.__get_column_name(cls)
            col_type, _ = Py2SQL.__sqlite_column_from_primitive(cls)
            query = '''CREATE TABLE IF NOT EXISTS {} ({} INTEGER PRIMARY KEY AUTOINCREMENT, {} {})'''\
                .format(table_name, PY2SQL_COLUMN_ID_NAME, col_name, col_type)
            self.cursor.execute(query)
        else:
            data_fields = Py2SQL.__get_data_fields_names(cls)
            if not all(list(map(lambda df: Py2SQL.__is_primitive_type(type(getattr(cls, df))),
                                data_fields))):
                query = '''CREATE TABLE IF NOT EXISTS {} ({} INTEGER PRIMARY KEY AUTOINCREMENT)'''\
                    .format(table_name, PY2SQL_COLUMN_ID_NAME)
                self.cursor.execute(query)
            else:
                # todo ? add all columns?
                query = '''CREATE TABLE IF NOT EXISTS {} ({} INTEGER PRIMARY KEY AUTOINCREMENT'''
                cols_query = ""
                for df in data_fields:
                    cols_query += ", "
                    col_name = Py2SQL.__get_column_name(None, df)
                    col_type, _ = Py2SQL.__sqlite_column_from_primitive(type(getattr(cls, df)))
                    cols_query += col_name + " " + col_type
                query = query + cols_query + ")"
                print(query)
                query_ex = query.format(table_name, PY2SQL_COLUMN_ID_NAME)
                self.cursor.execute(query_ex)

        self.connection.commit()

        return table_name, PY2SQL_COLUMN_ID_NAME

    @staticmethod
    def __get_data_fields_names(cls_obj):
        """
        Retrieves from class object data field names.

        Not includes magic attributes and functions (methods)
        :param cls_obj:
        :return: list(str, str,...)
        """
        data_attr_names = list()
        for k in cls_obj.__dict__.keys():
            if not Py2SQL.__is_magic_attr(k) and not isfunction(getattr(cls_obj, k))\
                    and PY2SQL_ID_NAME != k:
                data_attr_names.append(k)

        return data_attr_names

    def __create_or_update_col(self, table_name: str, cls, attr_name: str, attribute):
        """
        Creates or updates if exist column in table_name table
        :param table_name:
        :param cls: primitive type?
        :return:
        """
        if (Py2SQL.__is_primitive_type(type(attribute))):
            col_type, _ = Py2SQL.__sqlite_column_from_primitive(attribute.__class__)
            col_name = Py2SQL.__get_column_name(attribute.__class__, attr_name)
            query = "ALTER TABLE {} ADD COLUMN {} {}".format(table_name, col_name, col_type)
            try:
                self.cursor.execute(query)
                self.connection.commit()
            except Exception:
                pass
        else:
            mes = "Trying to create column from " + str(type(attribute)) + ". Primitive type expected"
            raise Exception(mes)

    @staticmethod
    def __get_foreign_key_name(attribute_name, reference_on_table_name) -> str:
        """

        :param attribute_name: class attribute name keeping not primitive attribute
        :param reference_on_table_name: table name for foreign key to reference on
        :return:
        """
        return "a_fk_" + attribute_name + "_" + reference_on_table_name

    def __add_foreign_key(self, fk_name: str, tbl_from: str, tbl_to: str) -> None:
        """
        Inserts foreign key column in the table.

        In SQLIte foreign key constaints cannot be added after table creation.
        So foreign key colums is line any common colon but with specific name

        :param fk_name:
        :param tbl_from:
        :param tbl_to:
        :return: None
        """
        foreign_type = "INTEGER"
        query = "ALTER TABLE {} ADD COLUMN {} {}".format(tbl_from, fk_name, foreign_type)
        try:
            self.cursor.execute(query)
            self.connection.commit()
        except Exception:
            pass

    def save_class(self, cls) -> 'class table name':
        """
        Save given class instance's representation into database or update it if it already exists

        Creates or updates tables structure to represent class object
        :param cls: class instance to be saved
        :return: class table name
        """
        if Py2SQL.__is_primitive_type(cls):
            (class_table_name, id_col) = self.__create_or_update_table(cls)
            return class_table_name

        tbl_name, id_name = self.__create_or_update_table(cls)

        # base classes contain also current class and object class
        base_classes = cls.__mro__
        for base_class in reversed(base_classes):
            if base_class == object:
                continue
            else:
                data_fields = Py2SQL.__get_data_fields_names(base_class)
                print(base_class.__name__ + " " + str(data_fields))
                for df_name in data_fields:
                    attribute = getattr(cls, df_name) # getting value!
                    attribute_type = type(attribute)
                    print(attribute_type)
                    if Py2SQL.__is_primitive_type(attribute_type):
                        self.__create_or_update_col(tbl_name, cls, df_name, attribute)
                    else:
                        parent_table_name = self.save_class(getattr(cls, df_name).__class__)
                        fk_name = Py2SQL.__get_foreign_key_name(df_name, parent_table_name)
                        self.__add_foreign_key(fk_name, tbl_name, parent_table_name)
        return tbl_name


    def save_hierarchy(self, root_class) -> None:
        """
        Saves all classes derived from root_class and classes these classes depends on

        :param root_class: Base class to save with all derived classes
        :return: None
        """
        self.save_class(root_class)
        subclasses = root_class.__subclasses__()
        if len(subclasses) == 0:
            return
        for c in subclasses:
            self.save_hierarchy(c)

    def delete_object(self, obj) -> None:
        """
        Delete given object instance's representation from database if it already existed

        :param obj: object instance to be deleted
        :return: None
        """
        table_name = Py2SQL.__get_object_table_name(obj)
        self.cursor.execute('DELETE FROM {} WHERE ID = {};'.format(table_name, id(obj)))
        if len(self.cursor.execute('SELECT ID FROM {}'.format(table_name)).fetchall()) == 0:
            self.cursor.execute('DROP TABLE {}'.format(table_name))

        if hasattr(obj, '__dict__'):  # object
            for value in obj.__dict__.values():
                if not Py2SQL.__is_of_primitive_type(value):
                    self.delete_object(value)  # cascade delete

        self.connection.commit()

    def delete_class(self, cls) -> None:
        """
        Delete given class instance's representation from database if it already existed.

        Drops corresponding table.

        :param cls: object instance to be delete
        :return: None
        """
        tbl_name = Py2SQL.__get_class_table_name(cls)
        query = "DROP TABLE IF EXISTS {}".format(tbl_name)
        self.cursor.execute(query)
        self.connection.commit()

    def delete_hierarchy(self, root_class) -> None:
        """
        Deletes root_class representation from database with all derived classes.

        Drops class corresponding table and all derived classes corresponding tables.
        :param root_class: Class which representation to be deleted with all derived classes
        :return: None
        """
        # consider foreign key constaints! todo
        self.delete_class(root_class)
        subclasses = root_class.__subclasses__()
        if len(subclasses) == 0:
            return
        for c in subclasses:
            self.delete_hierarchy(c)

if __name__ == '__main__':
    database_filepath = 'example.db'
    # os.remove(database_filepath)

    py2sql = Py2SQL()
    py2sql.db_connect(database_filepath)
    # showcase_table_name = 'object_int'
    #
    # py2sql.save_object(1)
    #
    # f = 1.1
    # py2sql.save_object(f)
    # py2sql.delete_object(f)
    # py2sql.save_object(2.2)
    #
    # py2sql.save_object('some str')
    # py2sql.save_object([1, 2])
    # py2sql.save_object((1, 2))
    # py2sql.save_object({1, 2})
    # py2sql.save_object(frozenset((1, 2)))
    # py2sql.save_object({'key': 'str', 1: 'int', (1, 2, 3): 'tuple'})
    #
    # a = array('i', [1, 2])
    # py2sql.save_object(a)
    #
    # sc = SampleClass(4)
    # py2sql.save_object(sc)
    # py2sql.delete_object(sc)
    # py2sql.save_object(SampleClass())
    #
    # print('Engine:', py2sql.db_engine())
    # print('Name:', py2sql.db_name())
    # print('Size in Mb:', py2sql.db_size())
    # print('Tables:', py2sql.db_tables())
    # print('{} table structure:'.format(showcase_table_name), py2sql.db_table_structure(showcase_table_name))
    # print('{} table size:'.format(showcase_table_name), py2sql.db_table_size(showcase_table_name))
    #
    # py2sql._Py2SQL__create_or_update_table(int)
    # py2sql.save_class(C)
    # py2sql.save_class(B)
    # py2sql.save_class(tuple)
    # py2sql.save_hierarchy(E)
    # py2sql.delete_hierarchy(E)
    py2sql.db_disconnect()
