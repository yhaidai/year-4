import os
import sqlite3
from array import array
from inspect import *

from demo_classes import SampleClass


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
    def __is_magic_attr(attr_name):
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
    def __get_class_table_name(cls_obj):
        """Defines database table name for class representation

            :return str
        """

        if Py2SQL.__is_of_primitive_type(cls_obj):
            return 'object_' + cls_obj.__name__
        return 'class_' + cls_obj.__name__

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
        # todo
        table_name = self.__get_class_table_name(cls)
        if (self.__is_primitive_type(cls)):
            pass
        else:
            pass

        return "table_name", "ID"

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
            if not Py2SQL.__is_magic_attr(k) and not isfunction(getattr(cls_obj, k)):
                data_attr_names.append(k)

        return data_attr_names

    def __create_or_update_col(self, table_name):
        """
        Creates or updates if exist colunb in table_name table
        :param table_name:
        :return:
        """
        pass

    def save_class(self, cls) -> None:
        """
        Save given class instance's representation into database or update it if it already exists

        Creates ooupdates tables structure to represent class object
        :param cls: class instance to be saved
        :return: None
        """
        if Py2SQL.__is_primitive_type(cls):
            # check if table exists and create if not
            class_table_name = Py2SQL.__get_class_table_name(cls)
            if not self.__table_exists(class_table_name):
                self.__create_or_update_table(cls)
                return

        # create table or check if exists todo
        (tbl_name, id_name) = self.__create_or_update_table(cls)

        # base classes contain also current class
        base_classes = cls.__mro__
        for base_class in reversed(base_classes):
            if base_class == object:
                continue
            else:
                data_fields = Py2SQL.__get_data_fields(base_class)
                for df in data_fields:
                    if Py2SQL.__is_primitive_type(type(getattr(cls, df))):
                        self.__create_or_update_col(tbl_name)
                    else:
                        # create foreign id column, get tablename and id from save_class() method
                        # so that foreign key could be generated
                        Py2SQL.save_class(getattr(cls, df))

    def save_hierarchy(self, root_class) -> None:
        pass

    def delete_object(self, obj) -> None:
        """
        Delete given object instance's representation from database if it already existed

        :param obj: object instance to be saved
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
        Delete given class instance's representation from database if it already existed

        :param cls: object instance to be saved
        :return: None
        """
        pass

    def delete_hierarchy(self, root_class) -> None:
        pass

if __name__ == '__main__':
    database_filepath = 'example.db'
    os.remove(database_filepath)

    py2sql = Py2SQL()
    py2sql.db_connect(database_filepath)
    showcase_table_name = 'object_int'

    py2sql.save_object(1)

    f = 1.1
    py2sql.save_object(f)
    py2sql.delete_object(f)
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
    py2sql.delete_object(sc)
    py2sql.save_object(SampleClass())

    print('Engine:', py2sql.db_engine())
    print('Name:', py2sql.db_name())
    print('Size in Mb:', py2sql.db_size())
    print('Tables:', py2sql.db_tables())
    print('{} table structure:'.format(showcase_table_name), py2sql.db_table_structure(showcase_table_name))
    print('{} table size:'.format(showcase_table_name), py2sql.db_table_size(showcase_table_name))

    py2sql.db_disconnect()