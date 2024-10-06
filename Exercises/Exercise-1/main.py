import os
import pathlib
import zipfile
import requests
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor, as_completed

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


async def main():
    os.makedirs("downloads", exist_ok=True)
    os.chdir("downloads")

    print("Downloading the data... ")
    failed_urls = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(download_with_requests, url): url for url in download_uris
        }
        for future in as_completed(futures):
            try:
                if future.result():
                    failed_urls.append(future.result())
            except Exception as ex:
                print(f"Error while downloading data: {ex}")
    print("done.")
    if not failed_urls:
        print("All URLs processed successfully!")
    else:
        print(f"{len(failed_urls)} URL(s) weren't processed because of errors:")
        [
            print(f"\t{entry[0]}: status {entry[1]}, reason '{entry[2]}'")
            for entry in failed_urls
        ]

    print("Extracting the data... ")
    for entry in pathlib.Path(os.getcwd()).iterdir():
        if entry.is_file() and entry.suffix == ".zip":
            try:
                with zipfile.ZipFile(entry.name) as zip_ref:
                    zip_ref.extractall(".")
            except Exception as ex:
                print(f"Couldn't extract the data from {entry.name}! The error is {ex}")

            try:
                entry.unlink()
            except Exception as ex:
                print(f"Couldn't delete the {entry.name} archive! The error is {ex}")
    print("done.")


def download_with_requests(url) -> tuple[str, int, str]:
    print(f"Downloading {url}... ")
    fname = url.split("/")[-1]
    if os.path.exists(fname):
        return
    resp = requests.get(url)
    if resp.status_code == 200:
        with open(fname, mode="x+b") as file:
            file.write(resp.content)
    else:
        return url, resp.status_code, resp.text


async def download_async(url) -> tuple[str, int, str]:
    print(f"Downloading {url}... ")
    fname = url.split("/")[-1]
    if os.path.exists(fname):
        return
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                with open(fname, mode="x+b") as file:
                    file.write(await resp.read())
            else:
                return (url, resp.status, await resp.text())


if __name__ == "__main__":
    asyncio.run(main())
