import io
import os
import requests
import pandas
import bs4
from urllib.parse import urlparse


def main():
    csv_link = find_csv_link("https://catalog.data.gov/dataset/air-quality")
    dl_result = download_csv(csv_link)
    if isinstance(dl_result, tuple):
        print(
            f"Failed to download {dl_result[0]} with status {dl_result[1]}. Error is {dl_result[2]}"
        )
        return
    df = pandas.read_csv(dl_result)
    biggest_vals = df.nlargest(5, "Data Value")
    print("Top 5 biggest values:")
    for i, row in enumerate(biggest_vals.itertuples(index=False), start=1):
        print(f"{i}. {row._10}: measured on {row.Start_Date} in {row._7}")


def find_csv_link(url):
    resp = requests.get(url)
    if resp.status_code != 200:
        raise Exception(
            f"Can't download the page! Status: {resp.status_code}, error: {resp.text}"
        )
    soup = bs4.BeautifulSoup(resp.text, "html.parser")
    csv_links = soup.find_all("a", title="Comma Separated Values File")
    if len(csv_links) == 0:
        raise Exception("Can't find the CSV link")
    if len(csv_links) > 1:
        raise Exception("More than one CSV links")
    btn_div = csv_links[0].parent.find("div", attrs={"class": "btn-group"})
    if btn_div is None:
        raise Exception("Can't find the CSV download button")
    csv_link = btn_div.find("a", attrs={"data-format": "csv"})["href"]
    return csv_link


def download_csv(url) -> tuple[str, int, str] | str:
    print(f"Downloading {url}... ")
    fname = urlparse(url).path.split("/")[-1]
    resp = requests.get(url)
    if resp.status_code == 200:
        with open(fname, mode="w+b") as file:
            file.write(resp.content)
        return fname
    else:
        return url, resp.status_code, resp.text


if __name__ == "__main__":
    main()
