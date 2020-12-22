from datetime import timedelta


def specialty_list_generator(c):
    class Wrapper:
        def __init__(self, *args):
            self.wrapper = c(*args)

            if hasattr(self.wrapper, 'spec'):
                try:
                    with open(self.wrapper.spec + '.txt') as f:
                        unique_programs = list(map(lambda x: x[:-1] if x.endswith('\n') else x, f.readlines()))
                except FileNotFoundError:
                    unique_programs = []

                with open(self.wrapper.spec + '.txt', 'w') as f:
                    if self.wrapper.prog not in unique_programs:
                        unique_programs.append(self.wrapper.prog)
                    f.write('\n'.join(sorted(unique_programs)))

    return Wrapper


class CreationLogger(type):
    def __init__(cls, *args, **kwargs):
        super().__init__(*args, **kwargs)
        cls.__reprs_to_instances = {}

    def __call__(cls, *args, **kwargs):
        instance_repr = ', '.join([str(arg) for arg in args])
        if instance_repr in cls.__reprs_to_instances:
            msg = f'{cls.__name__} {instance_repr} already exists: {cls.__reprs_to_instances[instance_repr]}'
            return msg
        else:
            instance = super().__call__(*args, **kwargs)
            cls.__reprs_to_instances[instance_repr] = instance

            try:
                with open(f'enrollment_cost_{instance.cost}') as f:
                    stored = list(map(lambda x: x[:-1] if x.endswith('\n') else x, f.readlines()))
            except FileNotFoundError:
                stored = []

            if instance_repr.replace(f', {str(instance.cost)}', '') not in stored:
                with open(f'enrollment_cost_{instance.cost}', 'a+') as f:
                    f.write(f'{instance.spec}, {instance.prog}, {instance.cred}, {instance.dur}\n')
            return instance


@specialty_list_generator
class BachelorDegree(metaclass=CreationLogger):
    def __init__(self, spec=str(), prog=str(), cred=int(), dur=timedelta(), cost=float()):
        self.spec = spec
        self.prog = prog
        self.cred = cred
        self.dur = dur
        self.cost = cost

    def __getattr__(self, item):
        if item not in self.__dict__:
            return f'No such attr: {item} in object of class {self.__class__.__name__}'

    def __getattribute__(self, item):
        return object.__getattribute__(self, item)

    def __setattr__(self, key, value):
        if isinstance(value, str) and key in ('spec', 'prog') or \
                isinstance(value, int) and key == 'cred' or \
                isinstance(value, timedelta) and key == 'dur' and \
                value <= timedelta(days=365*4 + 1) or \
                isinstance(value, float) and key == 'cost' and value >= 0:
            self.__dict__[key] = value
        else:
            raise ValueError

    def __delattr__(self, item):
        del self.__dict__[item]


BachelorDegree('ISS', 'iss program1', 22, timedelta(20), 1900.0)
bd = BachelorDegree('ISS', 'iss program1', 22, timedelta(20), 1900.0)
bd2 = BachelorDegree('SA', 'sa program1', 22, timedelta(20), 1900.0)
bd1 = BachelorDegree('ISS', 'other iss program2', 22, timedelta(20), 1900.0)
print(bd)
