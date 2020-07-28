import csv
import os

import db_api
from db_api import DataBase, DBField, DBTable
from typing import Any, Dict, List, Type
import shelve
from dataclasses_json import dataclass_json
from dataclasses import dataclass

@dataclass_json
@dataclass
class DBField(db_api.DBField):
    name: str
    type: Type


@dataclass_json
@dataclass
class SelectionCriteria(db_api.SelectionCriteria):
    field_name: str
    operator: str
    value: Any


class DBTable(db_api.DBTable):
    name: str
    fields: List[DBField]
    key_field_name: str

    def __init__(self,table_name, fields, key_field_name):
        self.name=table_name
        self.fields=fields
        self.key_field_name=key_field_name


    def count(self) -> int:

        data_table = shelve.open(f"db_files/{self.name}.db")
        count_tables = len(data_table.keys())
        data_table.close()
        return count_tables

    def insert_record(self, values: Dict[str, Any]) -> None:

        if None == values.get(self.key_field_name):
            raise ValueError
        data_table = shelve.open(f"db_files/{self.name}.db",writeback=True)
        try:
            if data_table.get(str(values[self.key_field_name])):
                data_table.close()
                raise ValueError

            if len(self.fields)<len(values.keys()):
                data_table.close()
                raise ValueError
            data_table[str(values[self.key_field_name])] = {}
            for field in self.fields:
                if values.get(field.name):
                    data_table[str(values[self.key_field_name])][field.name] = values[field.name]
                else:
                    data_table[str(values[self.key_field_name])][field.name] = None
        finally:
            data_table.close()

    def delete_record(self, key: Any) -> None:
        data_table = shelve.open(f"db_files/{self.name}.db",writeback=True)
        try:
            if None == data_table.get(str(key)):
                data_table.close()
                raise ValueError
            else:
                del data_table[str(key)]
        finally:
            data_table.close()

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:

        data_table = shelve.open(f"db_files/{self.name}.db")
        for criter in criteria:
            if data_table[list(data_table.keys())[0]].get(criter.field_name)==None:
                 raise ValueError
        for record_in_table in data_table.keys():
            flag = 0
            for record in criteria:
                if data_table[list(data_table.keys())[0]].get(record.field_name):
                    if record.operator == '=':
                         record.operator = '=='
                    if not eval(f"{data_table[record_in_table][record.field_name]}{record.operator}{str(record.value)}"):
                        flag = 1

            if not flag:
                self.delete_record(record_in_table)
        data_table.close()

    def get_record(self, key: Any) -> Dict[str, Any]:
        data_table = shelve.open(f"db_files/{self.name}.db",writeback=True)
        if data_table.get(str(key)):
            d = data_table[str(key)]
            data_table.close()
            return d
        else:
            data_table.close()
            raise ValueError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        data_table = shelve.open(f"db_files/{self.name}.db", writeback=True)
        if data_table.get(str(key)) == None:
            raise ValueError
        if values.get(self.key_field_name):
            raise ValueError
        for key_value in values.keys():
            if data_table[str(key)].get(key_value) == None:
                raise ValueError
        data_table[str(key)].update(values)
        data_table.close()

    def query_table(self, criteria: List[SelectionCriteria]) -> List[Dict[str, Any]]:

        data_table = shelve.open(f"db_files/{self.name}.db")

        Selection_criteria_list = []

        for criter in criteria:
            if data_table[list(data_table.keys())[0]].get(criter.field_name)==None:
                raise ValueError
        for record_in_table in data_table.keys():
            flag = 0
            for record in criteria:
                if data_table[list(data_table.keys())[0]].get(record.field_name):
                    if record.operator == '=':
                        record.operator = '=='
                    try:
                        if not eval(f'str(data_table[record_in_table][record.field_name]){record.operator}str(record.value)'):
                            flag = 1
                    except NameError:
                        print("invalid Name")

            if not flag:
                Selection_criteria_list.append(data_table[record_in_table])
        data_table.close()
        return Selection_criteria_list

    def create_index(self, field_to_index: str) -> None:



class DataBase(db_api.DataBase):


    __dict_tables__ = {}

    def __init__(self):
        with shelve.open(os.path.join(db_api.DB_ROOT, "DB.db"), writeback=True) as db:
            for key in db:
                DataBase.__dict_tables__[str(key)] = DBTable(key, db[key][0], db[key][1])


    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:

        if DataBase.__dict_tables__.get(table_name):
            return DataBase.__dict_tables__[table_name]
        flag = 0
        for field in fields:
            if key_field_name == field.name:
                flag = 1
        if flag == 0:
            raise ValueError
        data_table = shelve.open(os.path.join(db_api.DB_ROOT, table_name + ".db"), writeback=True)
        data_table.close()
        db_table = DBTable(table_name, fields, key_field_name)

        DataBase.__dict_tables__[table_name] = db_table
        with shelve.open(os.path.join(db_api.DB_ROOT, "DB.db"), writeback=True) as db:
            db[table_name] = [fields, key_field_name]
        return db_table

    def num_tables(self) -> int:
        return len(DataBase.__dict_tables__.keys())

    def get_table(self, table_name: str) -> DBTable:
            if None==DataBase.__dict_tables__.get(table_name):
                raise ValueError
            return DataBase.__dict_tables__[table_name]


    def delete_table(self, table_name: str) -> None:


        if DataBase.__dict_tables__.get(table_name):
            for suffix in ['bak', 'dat', 'dir']:
                os.remove(db_api.DB_ROOT.joinpath(f'{table_name}.db.{suffix}'))
            with shelve.open(os.path.join(db_api.DB_ROOT, "DB.db"), writeback=True) as db:
                del db[table_name]
            DataBase.__dict_tables__.pop(table_name)
    def get_tables_names(self) -> List[Any]:
            return list(DataBase.__dict_tables__.keys())



    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError



