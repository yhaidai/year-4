def dec(c):
    class Wrapper:
        def __init__(self, *args):
            self.wrapper = c(*args)
            if hasattr(self.wrapper, 'dep'):
                with open(self.wrapper.dep + '.txt', 'a+') as f:
                    f.write(' '.join((self.wrapper.name, self.wrapper.surname, '\n')))

    return Wrapper


class UniqueValidator(type):
    def __init__(cls, *args, **kwargs):
        super().__init__(*args, **kwargs)
        cls.__reprs_to_instances = {}

    def __call__(cls, *args, **kwargs):
        instance_repr = ' '.join(args)
        if instance_repr in cls.__reprs_to_instances:
            return '{} {} already exists: {}'.format(cls.__name__, instance_repr, cls.__reprs_to_instances[
                instance_repr])
        else:
            instance = super().__call__(*args, **kwargs)
            cls.__reprs_to_instances[instance_repr] = instance
            return instance


@dec
class Employee(metaclass=UniqueValidator):
    def __init__(self, name, surname, dep):
        self.dep = dep
        self.name = name
        self.surname = surname


a1 = Employee('John', 'Brooks', 'dep1')
a4 = Employee('John', 'Brooks', 'dep1')
print(hash(a1) == hash(a4))
a3 = Employee('Jane', 'Wilkins', 'dep1')
a2 = Employee('Jill', 'Jones', 'dep2')
