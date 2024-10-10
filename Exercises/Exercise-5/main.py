import collections
import csv
import datetime
import pathlib
import typing

# import psycopg2

CREATE_TABLE_TEMPLATE = """
create table %s (
    %s,

)
"""


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


def main():
    tables = []
    for file in pathlib.Path("./data").iterdir():
        if file.suffix == ".csv":
            table = Table(file.stem)
            with open(file, newline="") as f:
                reader = csv.reader(f, skipinitialspace=True)
                header = next(reader)
                table.columns.append(TableColumn(header[0], None, True))
                for column in header[1:]:
                    table.columns.append(TableColumn(column, None))
                    # TODO check for foreign key
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
            tables.append(table)
    print(tables)


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
    # return psycopg2.connect(host=host, database=database, user=user, password=pas)


if __name__ == "__main__":
    main()
