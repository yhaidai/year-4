from array import array


class AggregatedClass:
    str_aggr_class_attr = 'aggregated class str'

    def __init__(self):
        self.set_object_attr = ('aggregated', 'set', )


class SampleSuperClass:
    int_super_class_attr = 10

    def __init__(self):
        self.tuple_object_attr = (10, 20, 30)


class SampleClass(SampleSuperClass):
    float_class_attr = 1.5

    def __init__(self, int_object_attr=2, list_object_attr=None):
        super().__init__()
        self.int_object_attr = int_object_attr
        self.float_object_attr = 2.5
        self.str_object_attr = 'object attr str'
        if list_object_attr is None:
            self.list_object_attr = [1, '2']
        self.dict_object_attr = {1: 'int', '2': 'str', 3.88: 'float'}
        self.array_object_attr = array('i', [1, 2, 3])
        self.aggregated_object_attr = AggregatedClass()


class A:
    a = int()


class B:
    n = int()


class C(A):
    b = B()
    nc = int()


class E:
    pass


class F(C, E):
    pass


class J(E):
    pass


class H(J):
    pass


if __name__ == '__main__':
    print(F.__dict__)
