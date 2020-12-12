"""
    Module responsible for decorator for classes to use in py2sql
"""

PY2SQL_ID_NAME = '___id'
PY2SQL_COLUMN_STUB_NAME = '___stub'
PY2SQL_COLUMN_STUB_TYPE = 'TEXT'

PY2SQL_COLUMN_ID_TYPE = "INTEGER"
PY2SQL_COLUMN_ID_NAME = "___id"

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

