from university_unit import UniversityUnit
from university_unit_type import UniversityUnitType


if __name__ == '__main__':
    u4 = UniversityUnit(UniversityUnitType.LAB, 'L1', [], 15, students=10)
    u5 = UniversityUnit(UniversityUnitType.LAB, 'L2', [], 25)
    u2 = UniversityUnit(UniversityUnitType.CHAIR, 'C1', [u4, u5])
    u3 = UniversityUnit(UniversityUnitType.CHAIR, 'C2', [], 60)
    u1 = UniversityUnit(UniversityUnitType.INSTITUTE, 'I1', [u2, u3])

    print(u1.__dict__)

    print(u1.get_unit_type())
    print(u1.get_unit_id())
    print(u5.get_parent_name())
    print(u1.get_employee_count())
    print(u1.get_subordinate_employee_count(u5))

    print(u1.get_lyceum_students())
    print(u1.get_students())
    print(u1.get_postgraduates())
    print(u1.get_subordinate_lyceum_students(u4))
    print(u1.get_subordinate_students(u4))
    print(u1.get_subordinate_postgraduates(u4))

    print(u1.get_subordinate_types())
    print(u2.get_super_types())

    print(u1.get_subordinates_count())

