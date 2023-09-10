import os
import sys
import requests
import xmltodict

STORAGE_URL = "https://com-courtlistener-storage.s3-us-west-2.amazonaws.com"


def latest(files: list[str]) -> str:
    files.sort()
    return files[-1]


def download(filename: str) -> None:
    print(f"Downloading {filename}...")
    outfile = f"data/{filename}"

    if os.path.exists(outfile):
        print(f"{filename} already exists.")
        print()
        return

    print()
    with open(f"data/{filename}", "wb") as dl_file:
        with requests.get(
            f"{STORAGE_URL}/bulk-data/{filename}", stream=True
        ) as dl_response:
            for chunk in dl_response.iter_content(chunk_size=8192):
                dl_file.write(chunk)


if __name__ == "__main__":
    response = requests.get(STORAGE_URL + "/?delimiter=/&prefix=bulk-data/")
    listing = xmltodict.parse(response.text)

    if listing["ListBucketResult"]["IsTruncated"] != "false":
        print("Truncated result. Script needs to be rebuilt.", file=sys.stderr)
        sys.exit(1)

    data_types = ["citations", "opinions", "opinion-clusters", "dockets", "courts"]

    type_listings = {}

    for t in data_types:
        type_listings[t] = []

    for file in listing["ListBucketResult"]["Contents"]:
        key = file["Key"]
        s3_filename = key.split("/")[-1]
        for t in data_types:
            if s3_filename.startswith(t):
                type_listings[t].append(s3_filename)

    for t in data_types:
        download(latest(type_listings[t]))
