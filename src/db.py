from db_api import DataBase, DBField, DBTable
from typing import Any, Dict, List, Type
import shelve


class myDBTable(DBTable):
    name: str
    fields: List[DBField]
    key_field_name: str

    def count(self) -> int:
        raise NotImplementedError

    def insert_record(self, values: Dict[str, Any]) -> None:
        data_table = shelve.open(self.name)
        data_table[values[self.key_field_name]] = values

    def delete_record(self, key: Any) -> None:
        raise NotImplementedError

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        raise NotImplementedError

    def get_record(self, key: Any) -> Dict[str, Any]:
        raise NotImplementedError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        raise NotImplementedError

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        raise NotImplementedError

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError
class myDataBase(DataBase):

    dict_tables = {}
    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        data_table = shelve.open(f"{table_name}.db")
        data_table.close()
        db_table = DBTable(table_name, fields, key_field_name)
        self.dict_tables[table_name] = db_table
        return db_table

    def num_tables(self) -> int:
        return len(self.dict_tables.keys())

    def get_table(self, table_name: str) -> DBTable:
        return self.dict_tables.get(table_name)
    def delete_table(self, table_name: str) -> None:
        raise NotImplementedError

    def get_tables_names(self) -> List[Any]:
        return List[self.dict_tables.keys()]

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError



