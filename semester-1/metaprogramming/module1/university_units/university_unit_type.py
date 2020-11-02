from enum import IntEnum


class UniversityUnitType(IntEnum):
    INSTITUTE, FACULTY, COLLEGE, LYCEUM, CHAIR, LAB, DEPARTMENT, CENTER = range(8)
