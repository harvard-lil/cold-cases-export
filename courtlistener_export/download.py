import os
import sys
import requests
import xmltodict

STORAGE_URL = "https://com-courtlistener-storage.s3-us-west-2.amazonaws.com"

if __name__ == "__main__":
    response = requests.get(STORAGE_URL + "/?delimiter=/&prefix=bulk-data/")
    listing = xmltodict.parse(response.text)

    if listing["ListBucketResult"]["IsTruncated"] != "false":
        print("Truncated result. Script needs to be rebuilt.", file=sys.stderr)
        sys.exit(1)

    citations = []
    opinions = []
    opinion_clusters = []

    for file in listing["ListBucketResult"]["Contents"]:
        key = file["Key"]
        s3_filename = key.split("/")[-1]

        if s3_filename.startswith("citations"):
            citations.append(s3_filename)

        elif s3_filename.startswith("opinion-clusters"):
            opinion_clusters.append(s3_filename)

        elif s3_filename.startswith("opinions"):
            opinions.append(s3_filename)

    def latest(files: list[str]) -> str:
        files.sort()
        return files[-1]

    latest_citations = latest(citations)
    latest_opinions = latest(opinions)
    latest_opinion_clusters = latest(opinion_clusters)

    def download(filename: str) -> None:
        print(f"Downloading {filename}...")
        outfile = f"data/{filename}"

        if os.path.exists(outfile):
            print(f"{filename} already exists.")
            return

        with open(f"data/{filename}", "wb") as dl_file:
            with requests.get(
                f"{STORAGE_URL}/bulk-data/{filename}", stream=True
            ) as dl_response:
                for chunk in dl_response.iter_content(chunk_size=8192):
                    dl_file.write(chunk)

    download(latest_citations)
    download(latest_opinion_clusters)
    download(latest_opinions)
