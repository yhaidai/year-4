from array import array


class SampleClass(object):
    float_class_attr = 1.5

    def __init__(self):
        self.int_object_attr = 2
        self.float_object_attr = 2.5
        self.str_object_attr = 'some str'
        self.list_object_attr = [1, '2']
        self.dict_object_attr = {1: 'int', '2': 'str', (3, ): 'tuple'}
        self.array_object_attr = array('i', [1, 2, 3])


if __name__ == '__main__':
    pass
