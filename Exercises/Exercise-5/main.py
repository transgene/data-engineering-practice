import csv
import datetime
import pathlib
import typing

import psycopg2


CREATE_TABLE_TEMPLATE = \
"""create table {name} (
{definitions}
)
"""
COLUMN_TEMPLATE = "    {name} {type} {nullable_opt}"
PRIMARY_KEY_TEMPLATE = "    primary key ({})"
FOREIGN_KEY_TEMPLATE = "    constraint fk_{col_name} foreign key ({col_name}) references {ref_table}({ref_col_name})"

SQL_TYPES_DICT = {
    "int": "integer",
    "float": "float",
    "bool": "boolean",
    "datetime": "timestamp",
    "str": "text",
}

INSERT_INTO_TEMPLATE = \
"""insert into {table} 
values 
{values}
"""

INSERT_VALUES_ROW_TEMPLATE = "    ({})"

class TableColumn:
    def __init__(self, name: str, col_type: typing.Any, is_primary_key: bool = False):
        self.name = name
        self.col_type = col_type
        self.is_primary_key = is_primary_key
        self.is_nullable = False


class Table:
    def __init__(self, name: str):
        self.name = name
        self.name_singular = name[:-1]
        self.columns = []
        self.rows = []
        self.foreign_keys = {}


def main():
    tables = {}
    foreign_key_registry = []
    for file in pathlib.Path("./data").iterdir():
        if file.suffix == ".csv":
            table = Table(file.stem)
            with open(file, newline="") as f:
                reader = csv.reader(f, skipinitialspace=True)
                header = next(reader)
                table.columns.append(TableColumn(header[0], None, True))
                for column in header[1:]:
                    table.columns.append(TableColumn(column, None))
                    if column.endswith("_id"):
                        foreign_key_registry.append((column, table, column[:-3]))
                while True:
                    row = []
                    csv_row = next(reader, None)
                    if not csv_row:
                        break
                    for i, value in enumerate(csv_row):
                        if value == "":
                            table.columns[i].is_nullable = True
                            row.append(None)
                        else:
                            guessed_value, guessed_type = convert(
                                value, table.columns[i].col_type
                            )
                            row.append(guessed_value)
                            table.columns[i].col_type = guessed_type
                    table.rows.append(row)
            tables[table.name_singular] = table

    for col_name, owning_table, ref_table_singular in foreign_key_registry:
        owning_table.foreign_keys[col_name] = tables[ref_table_singular]

    create_table_scripts = []
    insert_scripts = []
    sorted_tables = sorted(tables.values(), key=lambda t: len(t.foreign_keys))
    for table in sorted_tables:
        col_defs = []
        for column in table.columns:
            col_defs.append(
                COLUMN_TEMPLATE.format(
                    name=column.name,
                    type=SQL_TYPES_DICT[column.col_type.__name__],
                    nullable_opt="not null" if not column.is_nullable else "",
                ).rstrip()
            )
        col_defs.append(PRIMARY_KEY_TEMPLATE.format(table.columns[0].name))
        for col_name, ref_table in table.foreign_keys.items():
            col_defs.append(
                FOREIGN_KEY_TEMPLATE.format(
                    col_name=col_name,
                    ref_table=ref_table.name,
                    ref_col_name=ref_table.columns[0].name,
                )
            )
        create_table_scripts.append(
            CREATE_TABLE_TEMPLATE.format(
                name=table.name,
                definitions=",\n".join(col_defs)
            )
        )

        insert_rows = []
        for row in table.rows:
            values_wrapped = []
            for value in row:
                if value is None:
                    values_wrapped.append("NULL")
                elif isinstance(value, datetime.datetime):
                    values_wrapped.append(f"'{value.isoformat()}'")
                elif isinstance(value, bool):
                    values_wrapped.append(str(value).lower())
                elif isinstance(value, str):
                    values_wrapped.append(f"'{value}'")
                else:
                    values_wrapped.append(str(value))
            insert_rows.append(
                INSERT_VALUES_ROW_TEMPLATE.format(", ".join(values_wrapped))
            )
        insert_scripts.append(
            INSERT_INTO_TEMPLATE.format(
                table=table.name,
                values=",\n".join(insert_rows),
            )
        )

    with get_db_connection() as con:
        cur = con.cursor()
        for script in create_table_scripts:
            cur.execute(script)
        for script in insert_scripts:
            cur.execute(script)
        cur.close()
        con.commit()

        for table in tables.values():
            cur = con.cursor()
            cur.execute(f"select * from {table.name}")
            print(f"SELECTING from {table.name}:")
            for row in cur.fetchall():
                print(row)
            cur.close()
        


def convert(value, to_type=None) -> tuple:
    cast_funcs = {
        int: int,
        float: float,
        bool: bool_cast,
        datetime.datetime: datetime_cast,
        str: str,
    }

    if to_type is not None:
        return cast_funcs[to_type](value), to_type

    for python_type, func in cast_funcs.items():
        guessed_value = try_cast(value, func)
        if guessed_value is not None:
            return guessed_value, python_type

    raise ValueError(f"Couldn't convert value '{value}' even to str")


def try_cast(value, cast_func):
    try:
        return cast_func(value)
    except ValueError:
        return None


def bool_cast(val):
    if val.lower() == "true":
        return True
    elif val.lower() == "false":
        return False
    else:
        raise ValueError(val)


def datetime_cast(val):
    return datetime.datetime.strptime(val, "%Y/%m/%d")


def get_db_connection():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    return psycopg2.connect(host=host, database=database, user=user, password=pas)


if __name__ == "__main__":
    main()
