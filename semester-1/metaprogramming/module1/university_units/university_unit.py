from abc import ABC
from university_unit_type import UniversityUnitType


class UniversityUnit(ABC):
    unit_ids = {i: 0 for i in list(map(int, UniversityUnitType))}

    def __init__(self, unit_type: UniversityUnitType, name: str, subordination: list, employee_count=0,
                 lyceum_students=0, students=0, postgraduates=0):
        self.__unit_type = unit_type
        self.__unit_id = unit_type.name.capitalize()[0] + str(UniversityUnit.unit_ids[unit_type.value])
        UniversityUnit.unit_ids[unit_type.value] += 1
        self.name = unit_type.name.capitalize() + ' ' + name

        self.__subordination = subordination
        self.__set_parents_in_subordination()

        self.__employee_count = employee_count
        self.__employee_count = self.__get_employee_count_based_on_subordination()
        self.__subordinates_count = self.__get_subordinates_count_based_on_subordination()

        self.__lyceum_students = lyceum_students
        self.__students = students
        self.__postgraduates = postgraduates
        self.__lyceum_students, self.__students, self.__postgraduates = self.__get_students_based_on_subordination()

        self.__generate_subordinate_attrs()

    def __set_parents_in_subordination(self, parent=None):
        for subordinate in self.__subordination:
            subordinate.__set_parents_in_subordination(parent=self)

        self.__parent = parent

    def __generate_subordinate_attrs(self, caller=None, attr_name=''):
        if caller is None:
            caller = self
        attr_name += self.__unit_id

        for subordinate in self.__subordination:
            subordinate.__generate_subordinate_attrs(caller, attr_name)

        caller.__dict__[attr_name + '_employee_count'] = self.__employee_count
        caller.__dict__[attr_name + '_lyceum_students'] = self.__lyceum_students
        caller.__dict__[attr_name + '_students'] = self.__students
        caller.__dict__[attr_name + '_postgraduates'] = self.__postgraduates

    def __get_employee_count_based_on_subordination(self):
        if not self.__subordination:
            return self.__employee_count

        employee_count = 0

        for subordinate in self.__subordination:
            subordinate.__get_employee_count_based_on_subordination()
            employee_count += subordinate.__employee_count

        return employee_count

    def __get_subordinates_count_based_on_subordination(self):
        subordinates_count = 0
        for subordinate in self.__subordination:
            subordinate.__get_subordinates_count_based_on_subordination()
            subordinates_count += len(subordinate.__subordination) + 1

        return subordinates_count

    def __get_students_based_on_subordination(self):
        if not self.__subordination:
            return self.__lyceum_students, self.__students, self.__postgraduates

        lyceum_students, students, postgraduates = 0, 0, 0
        for subordinate in self.__subordination:
            subordinate.__get_students_based_on_subordination()
            lyceum_students += subordinate.__lyceum_students
            students += subordinate.__students
            postgraduates += subordinate.__postgraduates

        return lyceum_students, students, postgraduates

    def get_unit_type(self):
        return self.__unit_type

    def get_unit_id(self):
        return self.__unit_id

    def get_parent_name(self):
        if self.__parent:
            return self.__parent.name

        return 'This unit has no parent'

    def get_employee_count(self):
        return self.__employee_count

    def get_subordinate_employee_count(self, subordinate):
        for key, value in self.__dict__.items():
            if key.endswith(subordinate.__unit_id + '_employee_count'):
                return value

    def get_lyceum_students(self):
        return self.__lyceum_students

    def get_students(self):
        return self.__students

    def get_postgraduates(self):
        return self.__postgraduates

    def get_subordinate_lyceum_students(self, subordinate):
        for key, value in self.__dict__.items():
            if key.endswith(subordinate.__unit_id + '_lyceum_students'):
                return value

    def get_subordinate_students(self, subordinate):
        for key, value in self.__dict__.items():
            if key.endswith(subordinate.__unit_id + '_students'):
                return value

    def get_subordinate_postgraduates(self, subordinate):
        for key, value in self.__dict__.items():
            if key.endswith(subordinate.__unit_id + '_postgraduates'):
                return value

    def get_subordinate_types(self):
        types = set()

        if not self.__subordination:
            types = (self.__unit_type, )

        for subordinate in self.__subordination:
            types.add(tuple(subordinate.get_subordinate_types()))

        return types

    def get_super_types(self):
        result = set()
        current = self

        while current.__parent:
            result.add(self.__parent.__unit_type)
            current = current.__parent

        return result

    def get_subordinates_count(self):
        return self.__subordinates_count
