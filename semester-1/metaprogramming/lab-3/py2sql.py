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
import builtins

from util import *
from demo_classes import SampleClass
from demo_classes import *


class Py2SQL:
    def __init__(self):
        self.filename = None
        self.connection = None
        self.cursor = None

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

    def save_object(self, obj) -> int:
        """
        Save given object instance's representation into database or update it if it already exists

        :param obj: object instance to be saved
        :rtype: int
        :return: id of object instance that was saved
        """
        table_name = Py2SQL.__get_object_table_name(obj)

        if not self.__table_exists(table_name):
            self.save_class(type(obj))

        if not Py2SQL.__is_of_primitive_type(obj):  # object
            self.__add_object_attrs_columns(obj, table_name)

            values = []
            for attr_name, attr_value in obj.__dict__.items():
                if not Py2SQL.__is_of_primitive_type(attr_value):
                    contained_obj_id = self.save_object(attr_value)
                    # save association reference for associated object
                    values.append(Py2SQL.__get_association_reference(attr_value, contained_obj_id))
                else:
                    values.append(Py2SQL.__sqlite_repr(attr_value)[0])
        else:
            values = Py2SQL.__sqlite_repr(obj)

        columns = [column_name for _, column_name, _ in self.db_table_structure(table_name)
                   if column_name.startswith(PY2SQL_OBJECT_ATTR_PREFIX) or
                   column_name == PY2SQL_PRIMITIVE_TYPES_VALUE_COLUMN_NAME]

        query = 'INSERT OR REPLACE INTO {}({}) VALUES ({});'.format(
            table_name, str(columns)[1:-1],
            ('?,' * len(values))[:-1]
        )
        print(query, values)

        self.cursor.execute(query, values)
        self.connection.commit()

        return self.cursor.execute('SELECT last_insert_rowid()').fetchone()[0]

    def __redefine_id_function(self, my_id):
        """
        Replace id() global function so that it returns my_id

        To cancel effect of this func call __reset_id_function() method.
        :param my_id: value to be returned after id() call
        :return: my_id
        """

        def id(ob):
            return my_id

        globals()['id'] = id

    def __reset_id_function(self):
        globals()['id'] = builtins.id

    def save_object_with_update(self, obj):
        """
        Inserts or updates obj related data

        :param obj: object to be saved or updated in db
        :return: object of type util.ModelPy2SQL
        """

        w = None
        # todo check save_object to return rowid (ID)
        if type(obj) != ModelPy2SQL:
            new_id = self.save_object(obj)
            w = ModelPy2SQL(obj, new_id)
        else:
            # todo SELECT py_id by rowid(ID)
            python_obj_id = ""
            self.__redefine_id_function(python_obj_id)
            self.save_object(obj.obj)
            self.__reset_id_function()
        return w

    @staticmethod
    def __get_object_column_name(attr_name: str):
        return PY2SQL_OBJECT_ATTR_PREFIX + PY2SQL_SEPARATOR + attr_name

    @staticmethod
    def __get_class_column_name(attr_name: str):
        return PY2SQL_CLASS_ATTR_PREFIX + PY2SQL_SEPARATOR + attr_name

    @staticmethod
    def __get_association_reference(obj, ref_id):
        return PY2SQL_ASSOCIATION_REFERENCE_PREFIX + PY2SQL_SEPARATOR + Py2SQL.__get_object_table_name(obj) + \
               PY2SQL_SEPARATOR + str(ref_id)

    @staticmethod
    def __get_base_class_table_reference_name(cls):
        return PY2SQL_BASE_CLASS_REFERENCE_PREFIX + PY2SQL_SEPARATOR + Py2SQL.__get_class_table_name(cls)

    @staticmethod
    def __is_magic_attr(attr_name: str) -> bool:
        """
        Defines is given attribute name is built-in magic attribute name

        :param attr_name:
        :return: bool
        """
        return attr_name.startswith("__") and attr_name.endswith("__")

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
        if type(obj) == array:
            return (obj.typecode + str(list(obj)),)
        elif Py2SQL.__is_of_primitive_type(obj):
            return (str(obj),)
        else:  # object
            return tuple(obj.__dict__.values())

    @staticmethod
    def __is_of_primitive_type(obj) -> bool:
        """
        Check whether given object is of primitive type i.e. is represented by a single field in SQLite database, thus
        can be embedded into 'composite' objects

        :param obj: object to be type-checked
        :rtype: bool
        :return: True if object is of primitive type, False otherwise
        """
        return Py2SQL.__is_primitive_type(type(obj))

    @staticmethod
    def __is_primitive_type(cls_obj):
        """
        Checks if input class object belongs to primitive built-in types
        :param cls_obj:
        :return: bool
        """

        return cls_obj in (int, float, str, dict, tuple, list, set, frozenset, array)

    @staticmethod
    def __get_column_name(cls, attr_name="") -> str:
        if attr_name == "":
            return cls.__name__
        else:
            return attr_name

    @staticmethod
    def __get_object_table_name(obj) -> str:
        """
        Build name of the table which should store objects of the same type as given one
        :param obj: object to build respective table name from
        :rtype: str
        :return: name of table to store object in
        """
        return Py2SQL.__get_class_table_name(type(obj))

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

    def __add_object_attrs_columns(self, obj, table_name):
        for attr_name in obj.__dict__:
            try:
                self.cursor.execute(
                    'ALTER TABLE {} ADD COLUMN {} TEXT'.format(
                        table_name,
                        Py2SQL.__get_object_column_name(attr_name)
                    )
                )
            except sqlite3.OperationalError:  # column already exists
                pass

    def __create_or_update_table(self, cls):
        """
        Consider cls as primitive type or as class with primitive attributes
        :param cls: primitive type (int, str, ...,) or class with primitive attributes
        :return: table name, id column name
        """
        table_name = self.__get_class_table_name(cls)
        data_fields = Py2SQL.__get_data_fields_names(cls)

        fk_columns_query = ','.join(
            ['{} REFERENCES {}(ID)'.format(
                Py2SQL.__get_base_class_table_reference_name(b),
                Py2SQL.__get_class_table_name(b)) for b in cls.__bases__ if b != object] +
            ['{} TEXT'.format(Py2SQL.__get_class_column_name(df)) for df in data_fields]
        )
        if fk_columns_query:
            fk_columns_query = ', ' + fk_columns_query

        query = 'CREATE TABLE IF NOT EXISTS {} ({} INTEGER PRIMARY KEY AUTOINCREMENT {})' \
            .format(table_name, PY2SQL_COLUMN_ID_NAME, fk_columns_query)

        if self.__is_primitive_type(cls):
            query = 'CREATE TABLE IF NOT EXISTS {} ({} INTEGER PRIMARY KEY AUTOINCREMENT, {} TEXT)' \
                .format(table_name, PY2SQL_COLUMN_ID_NAME, PY2SQL_PRIMITIVE_TYPES_VALUE_COLUMN_NAME)

        print(query)
        self.cursor.execute(query)
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
            if not Py2SQL.__is_magic_attr(k) and not isfunction(getattr(cls_obj, k)) \
                    and PY2SQL_ID_NAME != k:
                data_attr_names.append(k)

        return data_attr_names

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

        for base_class in cls.__bases__:
            if not base_class == object:
                self.save_class(base_class)

        tbl_name, id_name = self.__create_or_update_table(cls)

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
        # consider foreign key constraints! todo
        self.delete_class(root_class)
        subclasses = root_class.__subclasses__()
        if len(subclasses) == 0:
            return
        for c in subclasses:
            self.delete_hierarchy(c)


if __name__ == '__main__':
    database_filepath = 'example.db'
    os.remove(database_filepath)

    py2sql = Py2SQL()
    py2sql.db_connect(database_filepath)
    # showcase_table_name = 'object_int'
    #
    py2sql.save_object(1)
    #
    # f = 1.1
    # py2sql.save_object(f)
    # py2sql.delete_object(f)
    py2sql.save_object(2.2)

    py2sql.save_object('some str')
    py2sql.save_object([1, 2])
    py2sql.save_object((1, 2))
    py2sql.save_object({1, 2})
    py2sql.save_object(frozenset((1, 2)))
    py2sql.save_object({'key': 'str', 1: 'int', (1, 2, 3): 'tuple'})

    a = array('i', [1, 2])
    py2sql.save_object(a)

    sc = SampleClass(4)
    py2sql.save_object(sc)
    py2sql.save_object(sc)
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
    py2sql.save_class(B)
    py2sql.save_class(F)
    # py2sql.save_class(tuple)
    # py2sql.save_hierarchy(A)
    # py2sql.delete_hierarchy(E)
    py2sql.db_disconnect()
