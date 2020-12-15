"""
    Module responsible for decorator for classes to use in py2sql
"""

PY2SQL_ID_NAME = '___id'
PY2SQL_COLUMN_STUB_NAME = '___stub'
PY2SQL_COLUMN_STUB_TYPE = 'TEXT'

PY2SQL_CLASS_METHOD_PREFIX = 'CLASS_METHOD'
PY2SQL_CLASS_ATTR_PREFIX = 'CLASS_ATTR'
PY2SQL_OBJECT_ATTR_PREFIX = 'OBJECT_ATTR'
PY2SQL_OBJECT_METHOD_PREFIX = 'OBJECT_METHOD'
PY2SQL_BASE_CLASS_REFERENCE_PREFIX = 'BASE_REF'
PY2SQL_ASSOCIATION_REFERENCE_PREFIX = 'ASSOCIATION_REF'
PY2SQL_SEPARATOR = '$'

PY2SQL_COLUMN_ID_TYPE = "INTEGER"
PY2SQL_COLUMN_ID_NAME = "ID"
PY2SQL_PRIMITIVE_TYPES_VALUE_COLUMN_NAME = 'value'
PY2SQL_OBJECT_PYTHON_ID_COLUMN_TYPE = 'INTEGER'
PY2SQL_OBJECT_PYTHON_ID_COLUMN_NAME = 'py_id'
PY2SQL_DEFAULT_CLASS_BOUND_ROW_ID = 1


def get_pk_attr(obj, suffix=''):
    pk_column_name = PY2SQL_ID_NAME + suffix
    if not suffix:
        counter = 0
    else:
        counter = int(suffix)

    if hasattr(obj, pk_column_name):
        return get_pk_attr(obj, suffix=str(counter + 1))

    return pk_column_name


def model_py2sql(c):
    """
    Decorator for data models.

    Use @model_py2sql to decorate your data class.
    Adds methods for working with ID

    :param c: class object
    :return: class object
    """
    def py2sql_set_id(self, id):
        self.___id = int(id)

    def py2sql_get_id(self):
        return self.___id

    setattr(c, 'py2sql_get_id', py2sql_get_id)
    setattr(c, 'py2sql_set_id', py2sql_set_id)
    setattr(c, '___id', int())
    return c


class ModelPy2SQL:
    """
    Class wrapper for user's class to add model's attributes.
    """
    __id = int()

    def __init__(self, obj, id_=0):
        self.obj = obj
        self.__id = id_

    def get_id(self):
        return self.__id

    def set_id(self, id_):
        """
        Setting id for object you want to update.

        If in the corresponding table is no row with such ID, new row will be inserted,
        else updated.
        """
        self.__id = id_
