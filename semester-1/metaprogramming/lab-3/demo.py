import os
from array import array
from inspect import *

from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense

from demo_classes import *
from py2sql import Py2SQL


def save_demo(cls, obj1, obj2, obj3, alter_func=lambda x: x):
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


def db_info_demo(table_name):
    print('Engine:', py2sql.db_engine())
    print('Name:', py2sql.db_name())
    print('Size in Mb:', py2sql.db_size())
    print('Tables:', py2sql.db_tables())
    print('{} table structure:'.format(table_name), py2sql.db_table_structure(table_name))
    print('{} table size:'.format(table_name), py2sql.db_table_size(table_name))


def demo_():
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
    py2sql.delete_object(sc1)
    # py2sql.save_object(SampleClass())
    #

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
    py2sql.db_disconnect()


if __name__ == '__main__':
    database_filepath = 'example.db'
    os.remove(database_filepath)

    py2sql = Py2SQL()
    py2sql.db_connect(database_filepath)

    # save_demo(int, 1, 1203984, -49435320)
    # save_demo(float, 2.0415, 213.32098, -4538.344)
    # save_demo(str, 'some str', 'some other str', 'yet another str', lambda x: x.upper())
    # save_demo(list,
    #           [1, 'some str inside list', {'some key': (22, 33, {44, 55})}],
    #           ['to be deleted', {'some key': (22, 33, {44, 55})}],
    #           [frozenset(array('i', [100, 200, 300]))],
    #           lambda x: x.append('last element')
    #           )
    # save_demo(tuple,
    #           (1, 'some str inside tuple', [array('I', [12, 34, 56]), {78, 90}]),
    #           ('to be deleted', [array('I', [12, 34, 56]), {78, 90}]),
    #           ((frozenset([123, 'fs str']), 34), [111.23, 'list str', 3345])
    #           )

    d1 = Dense(units=128, activation='relu')
    d2 = Dense(units=64, activation='relu')
    d3 = Dense(units=16, activation='sigmoid')

    print(ismethoddescriptor(type(Sequential.layers).getter))
    print(Sequential.__bases__)
    print(Sequential.__dict__)
    print(Sequential.__name__)
    py2sql.save_class(Sequential)

    save_demo(Sequential,
              Sequential(d1, d2),
              Sequential(d1),
              Sequential(d3),
              lambda m: m.add(Dense(units=10, activation='softmax'))
              )

    py2sql.db_disconnect()
