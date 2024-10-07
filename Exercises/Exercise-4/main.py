import csv
import glob
import json
import os
import pathlib


def main():
    converted_dir = pathlib.Path("converted")
    if converted_dir.exists():
        os.removedirs(converted_dir)

    json_paths = glob.glob("data/**/*.json", recursive=True)

    converted_dir.mkdir()
    for json_path in json_paths:
        with open(json_path) as json_file:
            data = json.load(json_file)
            # with open(f"converted/{pathlib.Path(json_path).stem}.csv", "w") as csv_file:
            #     writer = csv.writer(csv_file)
            #     for row in data:
            #         writer.writerow(row)


if __name__ == "__main__":
    main()
