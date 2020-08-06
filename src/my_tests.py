import pytest

from db import DataBase
from db_api import DBField, SelectionCriteria, DB_ROOT, DBTable

STUDENT_FIELDS = [DBField('ID', int), DBField('First', str),
                  DBField('Last', str), DBField('Birthday', str)]

db = DataBase()


def create_students_table(db: DataBase, num_students: int = 0) -> DBTable:
    table = db.create_table('Students_index', STUDENT_FIELDS, 'ID')
    for i in range(num_students):
        add_student(table, i)
    return table


def add_student(table: DBTable, index: int, **kwargs) -> None:
    info = dict(
        ID=1_000_000 + index,
        First=f'John{index}',
        Last=f'Doe{index}',
        Birthday="23/11/2000"
    )
    info.update(**kwargs)
    table.insert_record(info)


def test_create_index():
    Students_index = create_students_table(db)
    assert db.num_tables() == 1
    assert db.get_tables_names() == ['Students_index']
    students = db.get_table('Students_index')
    add_student(students, 111, Birthday="ry")
    add_student(students, 123, Birthday="ry")
    add_student(students, 145, Birthday="h")
    add_student(students, 1435, Birthday="tr")
    assert students.count() == 4
    add_student(students, 151, Birthday="ry")
    students.delete_record(1_000_111)
    assert students.count() == 4
    with pytest.raises(ValueError):
         students.delete_record(key=1_000_111)
    students.create_index('Birthday')
    add_student(students, 142, Birthday="ry")
    students.delete_record(key=1000142)
    students.update_record(key=1000151, values={"Birthday": "update2"})
    results = students.query_table([SelectionCriteria('Birthday', '=', 'ry')])
    assert len(results) == 1
    students.update_record(key=1000145, values={"Birthday": "update1"})
    add_student(students, 121, Birthday="ry")
    students.update_record(1_000_123, dict(First='Jane', Last='Doe'))
    results = students.query_table([SelectionCriteria('Birthday', '=', 'ry')])
    assert len(results) == 2
    assert results[0]['Birthday'] == 'ry'
    with pytest.raises(ValueError):
        students.update_record(key=1000333, values={"Birthday": "update2"})
    students.delete_records([SelectionCriteria('Birthday', '=', 'ry')])
