import collections
import collections.abc
import csv
import glob
import json
import os
import pathlib
import shutil


def main():
    converted_dir = pathlib.Path("converted")
    if converted_dir.exists():
        shutil.rmtree(converted_dir, ignore_errors=True)

    json_paths = glob.glob("data/**/*.json", recursive=True)

    converted_dir.mkdir()
    for json_path in json_paths:
        with open(json_path) as json_file:
            data = json.load(json_file)
            flat = flatten(data)
            with open(f"converted/{pathlib.Path(json_path).stem}.csv", "w") as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=flat.keys())
                writer.writeheader()
                writer.writerow(flat)


def flatten(json_dict: dict):
    return _do_flatten(None, json_dict)


def _do_flatten(prefix: str | None, value) -> dict:
    res = {}
    if isinstance(value, collections.abc.Mapping):
        for k, v in value.items():
            key = f"{prefix}_{k}" if prefix else k
            res.update(_do_flatten(key, v))
    elif isinstance(value, list):
        for i, v in enumerate(value, start=1):
            key = f"{prefix}_{i}" if prefix else str(i)
            res.update(_do_flatten(key, v))
    else:
        res[prefix] = value
    return res


if __name__ == "__main__":
    main()
