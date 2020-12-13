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
import sys
import logging

from util import *
from demo_classes import *


class Py2SQL:
    def __init__(self, logs_enabled=False, log_file=""):
        self.filename = None
        self.connection = None
        self.cursor = None

    def __setup_logger(self, logs_enabled: bool, log_file: str):
        """
        Creates and returns logger.

        :param logs_enabled: True to enable, False to disable
        :param log_file: absolute path with file name of file for logging to
        :return: logger instance from 'logging' module
        """
        logging.basicConfig(level=logging.DEBUG, filename=log_file, filemode="a")
        logger = logging.getLogger("main_logger")
        logger.addFilter(lambda r: bool(logs_enabled))
        return logger

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

    def db_table_size(self, table_name: str) -> float:
        """
        Dynamically calculates data size stored in the table with table name provided in Mb.

        :table_name: table name to get size of
        :rtype: float
        :return: size of table ib Mb
        """
        if not type(table_name) == str:
            raise ValueError("str type expected as table_name. Got " + str(type(table_name)))
        q = "SELECT * FROM {}".format(table_name)
        try:
            self.cursor.execute(q)
        except Exception:
            raise Exception('No table' + table_name + ' found')
        rows = self.cursor.fetchall()

        col_names = list(map(lambda descr_tuple: descr_tuple[0], self.cursor.description))
        int_size = 8
        text_charsize = 2
        bytes_size = 0
        for r in rows:
            for i in range(len(r)):
                if r[i] is None:
                    continue
                elif (col_names[i] == PY2SQL_COLUMN_ID_NAME) or (col_names[i] == PY2SQL_OBJECT_PYTHON_ID_COLUMN_NAME):
                    bytes_size += int_size
                elif type(r[i]) == int:
                    bytes_size += int_size
                elif type(r[i]) == str:
                    bytes_size += len(r[i]) * text_charsize
                else:
                    continue

        return float(bytes_size / 1024 / 1024)

    # Python -> SQLite

    def save_object(self, obj) -> int:
        """
        Save representation of given object instance into database or update it if it already exists

        :param obj: object instance to be saved
        :rtype: int
        :return: id of object instance that was saved
        """
        table_name = Py2SQL.__get_object_table_name(obj)

        if not self.__table_exists(table_name):
            self.save_class(type(obj))

        if not Py2SQL.__is_of_primitive_type(obj):  # object
            self.__add_object_attrs_columns(obj, table_name)

            values = [id(obj)]
            for attr_value in obj.__dict__.values():
                values.append(self.__get_sqlite_repr(attr_value))
        else:
            values = (id(obj), self.__get_sqlite_repr(obj))

        columns = self.__get_object_bound_columns(table_name)

        obj_pk = self.__get_pk_if_exists(obj)
        if obj_pk:
            query = 'UPDATE {} SET {} WHERE {} = ?'.format(
                table_name,
                ', '.join(['{} = ?'.format(c) for c in columns.split(', ')]),
                PY2SQL_COLUMN_ID_NAME
            )
            params = (*values, obj_pk)
            # print(query, params)
            self.cursor.execute(query, params)
            return obj_pk

        query = 'INSERT INTO {}({}) VALUES ({});'.format(
            table_name,
            columns,
            ('?,' * len(values))[:-1]
        )
        # print(query, values)

        self.cursor.execute(query, values)
        self.connection.commit()

        return self.__get_last_inserted_id()

    def __get_pk_if_exists(self, obj):
        """
        Retrieve primary key of given object from corresponding table

        :param obj: obj to get primary key of if it exists in corresponding table
        :rtype: int or None
        :return: primary key of object if it is in the table, otherwise None
        """
        table_name = Py2SQL.__get_object_table_name(obj)
        existed_id = self.cursor.execute(
            'SELECT {} FROM {} WHERE {} = ?'.format(
                PY2SQL_COLUMN_ID_NAME, table_name, PY2SQL_OBJECT_PYTHON_ID_COLUMN_NAME
            ),
            (str(id(obj)),)
        ).fetchone()

        if existed_id:
            return existed_id[0]
        return None

    def __get_last_inserted_id(self):
        """
        Retrieve last id inserted into the database

        :rtype: int
        :return: last id inserted into the database
        """
        return self.cursor.execute('SELECT last_insert_rowid()').fetchone()[0]

    @staticmethod
    def __get_object_column_name(attr_name: str):
        """
        Retrieve name of the column responsible for storing given object instance attribute

        :type attr_name: str
        :param attr_name: name of the object instance attribute to get the column name
        :return: name of the column responsible for storing given attribute
        """
        return PY2SQL_OBJECT_ATTR_PREFIX + PY2SQL_SEPARATOR + attr_name

    @staticmethod
    def __get_class_column_name(attr_name: str, attr_value) -> str:
        """
        Retrieve name of the column responsible for storing given class instance attribute

        :type attr_name: str
        :param attr_name: name of the class instance attribute to get the column name
        :param attr_value: value of the  class instance attribute to get the column name
        :rtype: str
        :return: name of the column responsible for storing given attribute
        """
        if isfunction(attr_value):
            return PY2SQL_CLASS_METHOD_PREFIX + PY2SQL_SEPARATOR + attr_name
        return PY2SQL_CLASS_ATTR_PREFIX + PY2SQL_SEPARATOR + attr_name

    @staticmethod
    def __get_association_reference(obj, ref_id):
        """
        Retrieve association reference string for a given object instance and its primary key i.e. a string
        that represents association relationship between two objects

        :param obj: object instance to get the association reference for
        :param ref_id: primary key of object instance to be referenced in the corresponding table
        :rtype: str
        :return: association reference string
        """
        return PY2SQL_ASSOCIATION_REFERENCE_PREFIX + PY2SQL_SEPARATOR + Py2SQL.__get_object_table_name(obj) + \
               PY2SQL_SEPARATOR + str(ref_id)

    @staticmethod
    def __get_base_class_table_reference_name(cls) -> str:
        """
        Retrieve base class reference string for a given class instance i.e. a string
        that represents inheritance relationship between two classes

        :param cls: class instance to get base class table reference for
        :rtype: str
        :return: base class table reference string
        """
        return PY2SQL_BASE_CLASS_REFERENCE_PREFIX + PY2SQL_SEPARATOR + Py2SQL.__get_class_table_name(cls)

    @staticmethod
    def __is_magic_attr(attr_name: str) -> bool:
        """
        Defines is given attribute name is built-in magic attribute name

        :param attr_name:
        :return: bool
        """
        return attr_name.startswith("__") and attr_name.endswith("__")

    def __get_sqlite_repr(self, obj) -> str or None:
        """
        Retrieve SQLite representation of given object

        All primitives are represented by respective type copy constructor call string with the actual value passed,
        so that object instances of primitive types can be easily recreated from the database via eval() function

        Composite objects are represented by association reference strings, whereas functions are represented with
        their source code

        :param obj: object to be represented in SQLite database
        :rtype: str or None
        :return: sqlite representation of an object to be stored in the respective database table
        """
        if obj is None:
            return None
        if type(obj) == array:
            return '{}("{}", {})'.format(type(obj).__name__, obj.typecode, list(obj))
        if type(obj) == frozenset:
            return str(obj)
        if type(obj) == str:
            return '{}("{}")'.format(type(obj).__name__, obj)
        elif Py2SQL.__is_of_primitive_type(obj):
            return '{}({})'.format(type(obj).__name__, obj)
        elif isfunction(obj):
            return getsource(obj).replace("'", '"')
        else:  # object
            return Py2SQL.__get_association_reference(obj, self.save_object(obj))

    @staticmethod
    def __is_of_primitive_type(obj) -> bool:
        """
        Check whether given object is of primitive type i.e. is represented by a single field in SQLite database, thus
        can be embedded into 'composite' objects

        :param obj: object instance to be type-checked
        :rtype: bool
        :return: True if object is of primitive type, False otherwise
        """
        return Py2SQL.__is_primitive_type(type(obj))

    @staticmethod
    def __is_primitive_type(cls):
        """
        Checks if input class object belongs to primitive built-in types

        :param cls: class instance to check
        :rtype: bool
        :return: True if class is primitive type, False otherwise
        """

        return cls in (int, float, str, dict, tuple, list, set, frozenset, array)

    @staticmethod
    def __get_object_table_name(obj) -> str:
        """
        Retrieve name of the table which should store objects of the same type as given one

        :param obj: object to build respective table name from
        :rtype: str
        :return: name of table to store object in
        """
        return Py2SQL.__get_class_table_name(type(obj))

    @staticmethod
    def __get_class_name_by_table_name(table_name: str) -> tuple:
        """
        Parses given table name to find out name of class this table was created for

        :param table_name: table name of class to get name of
        :return: tuple (<full_module_name>, <class_name>)
        """
        divider = '$'
        ind = table_name.rfind(divider)
        module = table_name[:ind].replace(divider, ".")
        class_name = table_name[ind + 1:]

        return module, class_name

    @staticmethod
    def __get_attribute_name(self, tbl_name, col_name) -> str:
        """
        DO NOT USE

        :param tbl_name: table the column taken from
        :param col_name: column name
        :return:
        """
        cls = Py2SQL.__get_class_object_by_table_name(tbl_name)
        attr_name = ""
        if Py2SQL.__is_primitive_type(cls):
            pass
        else:
            pass
        # PY2SQL_PRIMITIVE_TYPES_VALUE_COLUMN_NAME
        # PY2SQL_OBJECT_ATTR_PREFIX + PY2SQL_SEPARATOR
        # todo
        return attr_name

    @staticmethod
    def __get_class_object_by_table_name(tbl_name):
        """
        Returns class object of corresponding tbl name or raise an Exception

        :param tbl_name: table name to get corresponding class object of
        :return: class object
        """
        module_nm, cls_nm = Py2SQL._get_class_name_by_table_name(tbl_name)
        cls_obj = None
        try:
            cls_obj = getattr(sys.modules[module_nm], cls_nm)
        except (AttributeError, KeyError) as e:
            msg = 'No such class: ' + module_nm + "." + cls_nm
            raise Exception(msg)
        except Exception:
            raise Exception('Unpredictable error')

        return cls_obj

    @staticmethod
    def __get_class_table_name(cls) -> str:
        """
        Retrieve name of the database table used to represent given class

        :param cls: class instance to get table name for
        :rtype: str
        :return: name of the table that represents given class
        """
        prefix = cls.__module__.replace(".", "$") + "$"
        if Py2SQL.__is_of_primitive_type(cls):
            return prefix + cls.__name__
        return prefix + cls.__name__

    def __table_exists(self, table_name):
        """
        Check if table with table name exists in database

        :param table_name: table name
        :return: bool, exists or not
        """
        for tbl_name in self.db_tables():
            if tbl_name == table_name:
                return True
        return False

    def __add_object_attrs_columns(self, obj, table_name):
        """
        Add columns representing attributes of given object instance to the table with given name

        :param obj: object to add attributes of to the table
        :param table_name: name of the table to add columns into
        :return: None
        """
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

    @staticmethod
    def __get_data_fields(cls_obj):
        """
        Retrieves from class object data field names.

        Not includes magic attributes and functions (methods)
        :param cls_obj:
        :return: list of two-element tuples containing data field name and value respectively
        """
        return [(k, v) for k, v in cls_obj.__dict__.items() if (not Py2SQL.__is_magic_attr(k) or isfunction(v)) and
                PY2SQL_ID_NAME != k]

    def __table_is_empty(self, table_name) -> bool:
        """
        Check if table is empty

        :param table_name: name of the table to check
        :rtype: bool
        :return: True if table is empty, False otherwise
        """
        return self.cursor.execute('SELECT count(*) FROM {}'.format(table_name)).fetchone()[0] == 0

    def __get_object_bound_columns(self, table_name) -> str:
        """

        :param table_name: name of the table to get columns bound to object instances from
        :rtype: str
        :return: comma separated list of column names
        """
        columns = ', '.join([column_name for _, column_name, _ in self.db_table_structure(table_name)
                             if column_name.startswith(PY2SQL_OBJECT_ATTR_PREFIX) or
                             column_name == PY2SQL_PRIMITIVE_TYPES_VALUE_COLUMN_NAME or
                             column_name == PY2SQL_OBJECT_PYTHON_ID_COLUMN_NAME])
        return columns

    def __update_table(self, cls):
        """
        Updates table of class cls

        :param cls:
        :return: None
        """
        table_name = Py2SQL.__get_class_table_name(cls)
        columns = self.__get_object_bound_columns(table_name)

        self.cursor.execute('ALTER TABLE {} RENAME TO {}$backup;'.format(table_name, table_name))
        self.__create_table(cls)
        self.cursor.execute('INSERT INTO {}({}) SELECT {} FROM {}$backup WHERE {} <> {};'.format(
            table_name,
            columns,
            columns,
            table_name,
            PY2SQL_OBJECT_PYTHON_ID_COLUMN_NAME,
            PY2SQL_DEFAULT_CLASS_BOUND_ROW_ID
        ))

        self.cursor.execute('DROP TABLE {}$backup;'.format(table_name))

    def __create_table(self, cls) -> str:
        """
        Create SQLite table representation for given class instance

        :param cls: class instance to create SQLite table representation for
        :rtype: str
        :return: name of the table created
        """
        table_name = self.__get_class_table_name(cls)
        query_start = 'CREATE TABLE IF NOT EXISTS {} ({} INTEGER PRIMARY KEY AUTOINCREMENT, {} {}' \
            .format(table_name,
                    PY2SQL_COLUMN_ID_NAME,
                    PY2SQL_OBJECT_PYTHON_ID_COLUMN_NAME,
                    PY2SQL_OBJECT_PYTHON_ID_COLUMN_TYPE
                    )

        if self.__is_primitive_type(cls):
            query = query_start + ', {} TEXT)'.format(PY2SQL_PRIMITIVE_TYPES_VALUE_COLUMN_NAME)
        else:
            data_fields = Py2SQL.__get_data_fields(cls)

            base_ref_columns = ['{} REFERENCES {}(ID) DEFAULT {}'.format(
                Py2SQL.__get_base_class_table_reference_name(b),
                Py2SQL.__get_class_table_name(b),
                PY2SQL_DEFAULT_CLASS_BOUND_ROW_ID
            ) for b in cls.__bases__ if b != object]

            attr_columns = ['{} TEXT DEFAULT \'{}\''.format(
                Py2SQL.__get_class_column_name(k, v),
                self.__get_sqlite_repr(v)
            ) for k, v in data_fields]

            columns = base_ref_columns + attr_columns

            columns_query = ', '.join(columns)
            if columns_query:
                columns_query = ', ' + columns_query

            query = query_start + ' ' + columns_query + ')'

        # print(query)
        self.cursor.execute(query)

        if not self.__is_primitive_type(cls):
            if self.__table_is_empty(table_name):
                self.cursor.execute('INSERT INTO {} DEFAULT VALUES'.format(table_name))

        return table_name

    def save_class(self, cls) -> None:
        """
        Save given class instance's representation into database or update it if it already exists

        Creates or updates tables structure to represent class object

        :param cls: class instance to be saved
        :return: None
        """
        table_name = Py2SQL.__get_class_table_name(cls)
        if not self.__table_exists(table_name):
            self.__create_table(cls)
        if not self.__is_primitive_type(cls):
            self.__update_table(cls)

        self.connection.commit()

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
        self.cursor.execute(
            'DELETE FROM {} WHERE {} = ?;'.format(table_name, PY2SQL_OBJECT_PYTHON_ID_COLUMN_NAME), (id(obj),)
        )

        if not Py2SQL.__is_of_primitive_type(obj):  # object
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

    def __redefine_id_function(self, my_id):
        """
        Replace id() global function so that it returns my_id
        To cancel effect of this func call __reset_id_function() method.

        Use carefully. Reflection used.
        :param my_id: value to be returned after id() call
        :return: my_id
        """
        def id(ob):
            return my_id
        globals()['id'] = id

    def __reset_id_function(self) -> None:
        """
        Sets global module attribute 'id' to built-in python id() function
        Use carefully. Reflection used.
        """
        globals()['id'] = builtins.id

    def __redefine_pyid_col_name(self) -> None:
        """
        Replaces some constant values from util module.
        To cancel effect of func call use __reset_pyid_col_name

        Use carefully. Reflection used.
        """
        global PY2SQL_OBJECT_PYTHON_ID_COLUMN_NAME
        PY2SQL_OBJECT_PYTHON_ID_COLUMN_NAME = str(PY2SQL_COLUMN_ID_NAME)

    def __reset_pyid_col_name(self) -> None:
        """
        Cancels the effect of __redefine_pyid_col_name method.

        Use carefully. Reflection used.
        """
        global PY2SQL_OBJECT_PYTHON_ID_COLUMN_NAME
        PY2SQL_OBJECT_PYTHON_ID_COLUMN_NAME = getattr(sys.modules['util'], 'PY2SQL_OBJECT_PYTHON_ID_COLUMN_NAME')

    def save_object_with_update(self, obj):
        """
        Inserts or updates obj related data by ID provided.

        Obj expected to be ModelPy2SQL instance object.
        If so, row is updated if provided ID exists, and fails otherwise.
        If not - object will be inserted or updated as provided

        :param obj: object to be saved or updated in db
        :return: object of type util.ModelPy2SQL
        """

        w = None
        if type(obj) != ModelPy2SQL:
            new_id = self.save_object(obj)
            w = ModelPy2SQL(obj, new_id)
        else:
            tbl_nm = Py2SQL.__get_object_table_name(obj.obj)
            q = "SELECT * FROM {} WHERE {}={}"\
                .format(tbl_nm, PY2SQL_COLUMN_ID_NAME, obj.get_id())
            self.cursor.execute(q)
            rows = self.cursor.fetchall()
            if len(rows) == 0:
                mes = "No " + str(obj.obj.__class__.__name__) + " instance objects in " + tbl_nm + " with id: " + str(obj.get_id())
                raise Exception(mes)

            self.__redefine_id_function(obj.get_id())
            self.__redefine_pyid_col_name()
            self.save_object(obj.obj)
            self.__reset_pyid_col_name()
            self.__reset_id_function()
            w = obj

        return w

    def get_object_by_id(self, table_name, id):
        pass

if __name__ == '__main__':
    database_filepath = 'example.db'
    os.remove(database_filepath)

    logfile = "logs.txt"
    py2sql = Py2SQL(True, logfile)

    py2sql.db_connect(database_filepath)
    # showcase_table_name = 'object_int'

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

    sc1 = SampleClass(4)
    py2sql.save_object(sc1)
    sc1.new_attr = 'ASSOCIATION_REF$demo_classes_AssociatedClass$2'  # naming collision will never occur!
    py2sql.save_object(sc1)

    # crash code !
    sc1.int_object_attr = 999
    m = ModelPy2SQL(sc1, 2)
    py2sql.save_object_with_update(m)
    # py2sql.delete_object(sc1)
    # py2sql.save_object(SampleClass())
    #
    # print('Engine:', py2sql.db_engine())
    # print('Name:', py2sql.db_name())
    # print('Size in Mb:', py2sql.db_size())
    # print('Tables:', py2sql.db_tables())
    # print('{} table structure:'.format(showcase_table_name), py2sql.db_table_structure(showcase_table_name))
    # print('{} table size:'.format(showcase_table_name), py2sql.db_table_size(showcase_table_name))
    #

    py2sql.save_class(B)
    B.new_attr = 22
    py2sql.save_class(B)
    B.new_attr = 33
    py2sql.save_class(B)
    py2sql.save_class(C)
    # py2sql.delete_class(C)
    # py2sql.save_class(F)
    # py2sql.save_class(tuple)
    # py2sql.save_hierarchy(A)
    # py2sql.delete_hierarchy(A)

    print(py2sql.db_table_size('demo_classes$SampleClass'))

    py2sql.db_disconnect()
