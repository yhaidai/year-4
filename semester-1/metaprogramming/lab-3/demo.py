import os
from datetime import datetime, timedelta, date

from tensorflow.keras.layers import Dense
from tensorflow.keras.models import Sequential, Model

from demo_classes import *
from py2sql import Py2SQL


def save_delete_demo(py2sql, cls, obj1, obj2, obj3, alter_func=lambda x: x):
    py2sql.save_class(cls)

    py2sql.save_object(obj1)
    # attempt to save already saved object
    py2sql.save_object(obj1)
    alter_func(obj1)
    py2sql.save_object(obj1)

    py2sql.save_object(obj2)
    py2sql.delete_object(obj2)
    # attempt to delete already deleted object
    py2sql.delete_object(obj2)

    py2sql.save_object(obj3)


def sample_class_demo(py2sql):
    sc1 = SampleClass(4)
    sc2 = SampleClass(12)
    py2sql.save_object(sc1)
    SampleClass.new_attr = 123
    sc1.new_attr = 'ASSOCIATION_REF$demo_classes_AssociatedClass$2'  # naming collision will never occur!
    py2sql.save_object(sc1)
    delattr(SampleClass, 'new_attr')
    py2sql.save_class(SampleClass)
    delattr(sc2, 'int_object_attr')
    py2sql.save_object(sc2)
    py2sql.delete_class(SampleClass)


def hierarchy_demo(py2sql):
    py2sql.save_hierarchy(date)
    py2sql.delete_hierarchy(date)


def db_info_demo(py2sql, table_name):
    print('Engine:', py2sql.db_engine())
    print('Name:', py2sql.db_name())
    print('Size in Mb:', py2sql.db_size())
    print('Tables:', py2sql.db_tables())
    print('{} table structure:'.format(table_name), py2sql.db_table_structure(table_name))
    print('{} table size:'.format(table_name), py2sql.db_table_size(table_name))


def demo(clear_db=True):
    database_filepath = 'example.db'
    if clear_db:
        try:
            os.remove(database_filepath)
        except FileNotFoundError:
            pass

    py2sql = Py2SQL()
    py2sql.db_connect(database_filepath)

    save_delete_demo(py2sql, int, 1, 1203984, -49435320)
    save_delete_demo(py2sql, float, 2.0415, 213.32098, -4538.344)
    save_delete_demo(py2sql, str, 'some str', 'some other str', 'yet another str', lambda x: x.upper())
    save_delete_demo(py2sql,
                     list,
                     [1, 'some str inside list', {'some key': (22, 33, {44, 55})}],
                     ['to be deleted', {'some key': (22, 33, {44, 55})}],
                     [frozenset(array('i', [100, 200, 300]))],
                     lambda x: x.append('last element')
                     )
    save_delete_demo(py2sql,
                     tuple,
                     (1, 'some str inside tuple', [array('I', [12, 34, 56]), {78, 90}]),
                     ('to be deleted', [array('I', [12, 34, 56]), {78, 90}]),
                     ((frozenset([123, 'fs str']), 34), [111.23, 'list str', 3345])
                     )

    d1 = Dense(units=128, activation='relu')
    d2 = Dense(units=64, activation='relu')
    d3 = Dense(units=16, activation='sigmoid')

    save_delete_demo(py2sql,
                     datetime,
                     datetime.now(),
                     datetime(2077, 1, 1),
                     datetime(2000, 4, 29),
                     lambda d: d + timedelta(days=10)
                     )

    save_delete_demo(py2sql,
                     Sequential,
                     Sequential(d1, d2),
                     Sequential(d1),
                     Sequential(d3),
                     lambda m: m.add(Dense(units=10, activation='softmax'))
                     )

    sample_class_demo(py2sql)

    hierarchy_demo(py2sql)

    py2sql.db_disconnect()


if __name__ == '__main__':
    demo()
