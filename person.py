from os import makedirs, remove, listdir
from urllib.request import urlopen
from io import BytesIO
from zipfile import ZipFile
from csv import reader
import pandas as pd


def downloadZipFiles(beginYear, endYear):
    for year in range(beginYear, endYear + 1):
        URL = f"https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.ZIP"
        makedirs(f"./{year}")

        with urlopen(URL) as zip_response:
            with ZipFile(BytesIO(zip_response.read())) as zfile:
                zfile.extractall(f"./{year}")

        remove(f"./{year}/{year}FD.xml")


def extractData(beginYear, endYear, create_csv):
    data = {
        "first_name": [],
        "last_name": [],
        "date": [],
        "doc_id": [],
        "url": [],
    }
    base_url = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/"
    document_id_set = set()

    for year in range(beginYear, endYear + 1):
        txt_file = listdir(f"./{year}")[0]

        with open((f"./{year}/{txt_file}"), "r") as file:
            for line in list(reader(file, delimiter="\t"))[1:]:
                # only docIds that start with 2 are PTRs or Periodic Transaction Reports with transaction data
                if line[8].startswith("2"):
                    if not line[8] in document_id_set:
                        data["first_name"].append(line[2])
                        data["last_name"].append(line[1])
                        data["date"].append(line[7])
                        data["doc_id"].append(line[8])
                        data["url"].append(base_url + f"{line[7][-4:]}/{line[8]}.pdf")
                        document_id_set.add(line[8])

    df = pd.DataFrame(data)

    person_table = df[["first_name", "last_name"]].drop_duplicates()
    person_table["person_id"] = [i for i in range(len(person_table))]
    person_table = person_table[["person_id", "first_name", "last_name"]]

    person_data = df[["date", "doc_id"]]

    relationship_table = df[["doc_id", "url"]]

    id_col = []

    for mainFName in df["first_name"]:
        for person_id, fname in zip(
            person_table["person_id"], person_table["first_name"]
        ):
            if mainFName == fname:
                id_col.append(person_id)
                break

    relationship_table["person_id"] = id_col

    if create_csv:
        if "data" in listdir("."):
            df.drop("date", axis=1).to_csv(
                "./data/personData.csv",
                index=False,
            )
        else:
            makedirs("./data")
            df.drop("date", axis=1).to_csv(
                "./data/personData.csv",
                index=False,
            )

    return person_data, person_table, relationship_table