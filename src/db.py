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

    def __init__(self, table_name, fields, key_field_name):
        self.name = table_name
        self.fields = fields
        self.key_field_name = key_field_name
        self.list_index = []

    def count(self) -> int:

        data_table = shelve.open(f"db_files/{self.name}.db")
        count_tables = len(data_table.keys())
        data_table.close()
        return count_tables

    def is_index(self, field):
        return field in self.list_index

    def add_to_index(self, db_index, values, field):
        try:
            db_index[values[field.name]].append(values[self.key_field_name])
        except:
            db_index[values[field.name]] = [values[self.key_field_name]]

    def check_validate(self, values, data_table):
        if data_table.get(str(values[self.key_field_name])):
            data_table.close()
            raise ValueError

        if len(self.fields) < len(values.keys()):
            data_table.close()
            raise ValueError

        data_table[str(values[self.key_field_name])] = {}
        for field in self.fields:
            if self.is_index(field.name):
                if not values.get(field.name):
                    raise ValueError

    def insert_record(self, values: Dict[str, Any]) -> None:
        if not values.get(self.key_field_name):
            raise ValueError
        data_table = shelve.open(f"db_files/{self.name}.db", writeback=True)
        self.check_validate(values, data_table)
        for field in self.fields:
            if self.is_index(field.name):
                db_index = shelve.open(f"db_files/{self.name}_{field.name}", writeback=True)
                try:
                    db_index[values[field.name]].append(values[self.key_field_name])
                except:
                    db_index[values[field.name]] = [values[self.key_field_name]]
                finally:
                    db_index.close()
            if values.get(field.name):
                data_table[str(values[self.key_field_name])][field.name] = values[field.name]
            else:
                data_table[str(values[self.key_field_name])][field.name] = None
        data_table.close()

    def delete_record(self, key: Any) -> None:
        data_table = shelve.open(f"db_files/{self.name}.db", writeback=True)
        try:
            if not data_table.get(str(key)):
                data_table.close()
                raise ValueError
            else:
                for field in self.list_index:
                    db_index = shelve.open(f"db_files/{self.name}_{field}", writeback=True)
                    if len(db_index[data_table[str(key)][field]]) == 1:
                        del db_index[data_table[str(key)][field]]
                    else:
                        index_to_remove = db_index[data_table[str(key)][field]].index(key)
                        db_index[data_table[str(key)][field]].pop(index_to_remove)
                    db_index.close()
                del data_table[str(key)]
        finally:
            data_table.close()

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:

        records_to_delete = self.query_table(criteria)
        for record in records_to_delete:
            self.delete_record(record[self.key_field_name])

    def get_record(self, key: Any) -> Dict[str, Any]:
        data_table = shelve.open(f"db_files/{self.name}.db", writeback=True)
        if data_table.get(str(key)):
            record = data_table[str(key)]
            data_table.close()
            return record
        else:
            data_table.close()
            raise ValueError

    def check_validate_update(self, data_table, values, key):
        if not data_table.get(str(key)):
            raise ValueError
        if values.get(self.key_field_name):
            raise ValueError
        for key_value in values.keys():
            if not data_table[str(key)].get(key_value):
                raise ValueError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        data_table = shelve.open(f"db_files/{self.name}.db", writeback=True)
        self.check_validate_update(data_table, values, key)
        for field in self.list_index:
            if values.get(field):
                with shelve.open(f"db_files/{self.name}_{field}", writeback=True) as db_index:
                    if len(db_index[data_table[str(key)][field]]) == 1:
                        del db_index[data_table[str(key)][field]]
                    else:
                        db_index[data_table[str(key)][field]].remove(key)
                    try:
                        db_index[values[field]].append(key)
                    except:
                        db_index[values[field]] = [key]
        data_table[str(key)].update(values)
        data_table.close()

    def is_criteria(self, critery, key):
        if critery.operator == '=':
            critery.operator = '=='
        return not eval(f'str(key){critery.operator}str(critery.value)')

    def is_query_by_index(self, critery):
        is_critery = set()
        with shelve.open(f"db_files/{self.name}_{critery.field_name}", writeback=True) as db_index:
            for key in db_index.keys():
                try:
                    if not self.is_criteria(critery, key):
                        if is_critery == set():
                            is_critery = set(db_index[key])
                        else:
                            is_critery.intersection_update(set(db_index[key]))
                except NameError:
                    print("invalid Name")
        return is_critery

    def is_query_exist(self, list_keys, list_criteria):
        data_table = shelve.open(f"db_files/{self.name}.db")
        selection_criteria_list = []
        for record in list_keys:
            flag = 0
            for criteria in list_criteria:
                if data_table[list(data_table.keys())[0]].get(criteria.field_name):
                    try:
                        if self.is_criteria(criteria, data_table[record][criteria.field_name]):
                            flag = 1
                    except NameError:
                        print("invalid Name")
            if not flag:
                selection_criteria_list.append(data_table[record])
        data_table.close()
        return selection_criteria_list

    def check_validate_quary(self, data_table, criteria):
        for criter in criteria:
            if not data_table[list(data_table.keys())[0]].get(criter.field_name):
                raise ValueError

    def query_table(self, criteria: List[SelectionCriteria]) -> List[Dict[str, Any]]:
        data_table = shelve.open(f"db_files/{self.name}.db")
        self.check_validate_quary(data_table, criteria)
        query_set = set()
        criter_no_index = []
        for criter in criteria:
            if self.is_index(criter.field_name):
                if query_set == set():
                    query_set = self.is_query_by_index(criter)
                else:
                    query_set.intersection_update(self.is_query_by_index(criter))
            else:
                criter_no_index.append(criter)
        if criter_no_index:
            if query_set == set():
                list_keys = data_table.keys()
            else:
                list_keys = list(query_set)
            selection_criteria_list = self.is_query_exist(list_keys, criter_no_index)
        else:
            list_select = []
            query_set = list(query_set)
            for key in query_set:
                list_select.append(data_table[str(key)])
            selection_criteria_list = list_select
        data_table.close()
        return selection_criteria_list

    def check_validate_field(self, field_to_index):
        flag = 0
        for field in self.fields:
            if field.name == field_to_index:
                flag = 1
                break
        if not flag:
            raise ValueError

    def create_index(self, field_to_index: str) -> None:

        if field_to_index == self.key_field_name:
            return
        self.check_validate_field(field_to_index)
        db_index = shelve.open(f"db_files/{self.name}_{field_to_index}", writeback=True)
        data_table = shelve.open(f"db_files/{self.name}.db")
        db = shelve.open(f"db_files/DB.db", writeback=True)
        for record_in_table in data_table.keys():
            try:
                db_index[data_table[record_in_table][field_to_index]].append(int(record_in_table))
            except:
                db_index[data_table[record_in_table][field_to_index]] = [int(record_in_table)]
        self.list_index.append(field_to_index)
        db[self.name][2].append(field_to_index)
        db_index.close()
        data_table.close()
        db.close()


class DataBase(db_api.DataBase):

    __dict_tables__ = {}

    def __init__(self):
        with shelve.open(os.path.join(db_api.DB_ROOT, "DB.db"), writeback=True) as db:
            for key in db:
                db_table = DBTable(key, db[key][0], db[key][1])
                db_table.list_index = db[key][2]
                DataBase.__dict_tables__[str(key)] = db_table

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
        db_table.list_index = []
        DataBase.__dict_tables__[table_name] = db_table
        with shelve.open(os.path.join(db_api.DB_ROOT, "DB.db"), writeback=True) as db:
            db[table_name] = [fields, key_field_name, list()]
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



