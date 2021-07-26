from urllib.request import urlopen
from io import BytesIO
from csv import DictWriter
from assetTypes import asset_types as assets
from numpy import NaN
import concurrent.futures
import pandas as pd
import fitz


def cleanUpResults(strings):
    copy = strings.copy()

    for s in strings:
        if s in ["$200?", "amount", "sp", "dc", "jt", "g", "f", "e", "d", "c", "b"]:
            copy.remove(s)
        elif "filing status:" in s:
            copy.remove(s)
        elif "filing id" in s:
            copy.remove(s)
        elif "subholding of:" in s:
            copy.remove(s)
        elif "location:" in s:
            copy.remove(s)
        elif "comments" in s:
            copy.remove(s)

    return copy


def shapeResults(strings, doc_id):
    data = []

    shape = {
        "doc_id": doc_id,
        "company": None,
        "type": None,
        "date": None,
        "amount_range": None,
        "description": None,
    }

    company = shape.copy()
    name = ""
    amount_range = ""

    for idx, s in enumerate(strings):
        if not company["company"]:
            if not name:
                check = s.split()
                if len(check) == 2 and check[0].isdigit() and len(check[1]) == 2:
                    continue
                elif "description:" in s:
                    continue
                elif check[-1].endswith(".") and not check[-1].startswith("l"):
                    continue
            if not s in ["p", "s", "s (partial)"]:
                name += f"{s} "
                continue
            else:
                company["company"] = name
                company["type"] = s
                continue
        if not company["date"]:
            dates = s.split()
            if len(dates) > 1:
                company["date"] = dates[1]
            else:
                company["date"] = strings[idx + 1]
            continue
        if not company["amount_range"]:
            if s.startswith("$"):
                amount_range += f"{s} "
                if idx == len(strings) - 1:
                    pass
                else:
                    continue
            elif not amount_range:
                continue

            company["amount_range"] = amount_range

            if "description:" in s:
                company["description"] = s
                data.append(company.copy())
                company = shape.copy()
                name = amount_range = ""
            else:
                data.append(company.copy())
                company = shape.copy()
                name = s
                amount_range = ""

    return data


def getTransactionData(args):
    year, doc_id = args

    # date is passed in as param year in format m/d/year so we slice last 4 of string as year
    year = year[-4:]
    URL = (
        f"https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc_id}.pdf"
    )
    cleaned_content = []

    # used for seperating transactional content from unnecessary content in PDF file
    starting_idx = None
    ending_idx = None

    try:
        with urlopen(URL) as res:
            with fitz.open(stream=BytesIO(res.read()), filetype="pdf") as pdf:
                for page in pdf.pages():
                    content = page.get_text().casefold().splitlines()
                    try:
                        starting_idx = content.index("$200?")
                        if not starting_idx == len(content) - 1:
                            content = content[starting_idx:]
                        else:
                            starting_idx = 0
                            content = content[starting_idx:]
                        try:
                            ending_idx = content.index(
                                "* for the complete list of asset type abbreviations, please visit https://fd.house.gov/reference/asset-type-codes.aspx."
                            )
                            content = content[:ending_idx]
                        except:
                            pass
                    except:
                        if starting_idx:
                            break
                        else:
                            try:
                                starting_idx = content.index("amount_range")
                                content = content[starting_idx:]
                                try:
                                    ending_idx = content.index("asset class details")
                                    content = content[:ending_idx]
                                except:
                                    if "comments" in content:
                                        ending_idx = content.index("comments")
                                        content = content[:ending_idx]
                                    elif "initial public offerings" in content:
                                        ending_idx = content.index(
                                            "initial public offerings"
                                        )
                                        content = content[:ending_idx]
                            except:
                                break

                    content = cleanUpResults(content)
                    cleaned_content.extend(content)

        return shapeResults(cleaned_content, doc_id)

    except:
        return []


def cleanDescription(df, create_csv=False, read_from_csv=False):
    if read_from_csv:
        df = pd.read_csv("./transactionData.csv")

    def trimDescription(cell):
        if "description:" in cell:
            cell = cell.split()[1:]
            cell = " ".join(cell)

        return cell

    df["description"] = df["description"].map(trimDescription, na_action="ignore")

    if create_csv:
        df.to_csv(
            "./transactionData.csv",
            index=False,
            columns=[
                "doc_id",
                "company",
                "type",
                "date",
                "amount_range",
                "description",
            ],
        )
    return df


def cleanName(df, create_csv=False, read_from_csv=False):
    if read_from_csv:
        df = pd.read_csv("./transactionData.csv")

    def rearrangeName(cell):
        company = cell.split()
        for string in cell.split():
            # first check if (ticker) is not grouped with other text
            if string.startswith("(") and string.endswith(")"):
                ticker = string[1:-1]
                company.remove(string)
                company.insert(0, ticker.upper())
            # catchall condition if ticker is in string
            elif "(" in string and ")" in string:
                left_paren = string.find("(")
                right_paren = string.find(")")
                ticker = string[left_paren + 1 : right_paren]
                company.remove(string)
                company.insert(0, ticker.upper())

        return " ".join(company)

    df["company"] = df["company"].map(rearrangeName)

    if create_csv:
        df.to_csv(
            "./transactionData.csv",
            index=False,
            columns=[
                "doc_id",
                "company",
                "type",
                "date",
                "amount_range",
                "description",
            ],
        )

    return df


def createTickerColumn(df, create_csv=False, read_from_csv=False):
    if read_from_csv:
        df = pd.read_csv("./transactionData.csv")

    tickers = []
    for cell in df["company"]:
        if cell.split()[0].isupper():
            tickers.append(cell.split()[0])
        else:
            tickers.append(NaN)

    df["ticker"] = tickers

    df = df[
        ["doc_id", "ticker", "company", "type", "date", "amount_range", "description"]
    ]

    if create_csv:
        df.to_csv(
            "./transactionData.csv",
            index=False,
            columns=[
                "doc_id",
                "ticker",
                "company",
                "type",
                "date",
                "amount_range",
                "description",
            ],
        )

    return df


def updateName(df, create_csv=False, read_from_csv=False):
    if read_from_csv:
        df = pd.read_csv("./transactionData.csv")

    def changeName(cell):
        strings = cell.split()
        if strings[0].isupper():
            cell = strings[1:]
            cell = " ".join(cell)

        return cell

    df["company"] = df["company"].map(changeName)

    if create_csv:
        df.to_csv(
            "./transactionData.csv",
            index=False,
            columns=[
                "doc_id",
                "ticker",
                "company",
                "type",
                "date",
                "amount_range",
                "description",
            ],
        )

    return df


def extractAssetType(df, create_csv=False, read_from_csv=False):
    if read_from_csv:
        df = pd.read_csv("./transactionData.csv")

    security_types = []

    def removeAssetTypeFromName(cell):
        company = cell.split()
        security_type = company[-1]
        val = None

        if security_type.startswith("[") and security_type.endswith("]"):
            val = assets.get(security_type[1:-1])
        if val:
            security_types.append(val)
            val = None
            company.remove(security_type)
        else:
            security_types.append(NaN)

        return " ".join(company)

    df["company"] = df["company"].map(removeAssetTypeFromName)

    df["asset"] = security_types

    df = df[
        [
            "doc_id",
            "ticker",
            "company",
            "asset",
            "type",
            "date",
            "amount_range",
            "description",
        ]
    ]

    if create_csv:
        df.to_csv(
            "./transactionData.csv",
            index=False,
            columns=[
                "doc_id",
                "ticker",
                "company",
                "asset",
                "type",
                "date",
                "amount_range",
                "description",
            ],
        )

    return df


def capitalize(df, create_csv=False, read_from_csv=False):
    if read_from_csv:
        df = pd.read_csv("./transactionData.csv")

    def helper(cell):
        try:
            cell = cell.upper()
        except:
            pass

        return cell

    df[["company", "type", "description"]] = df[
        ["company", "type", "description"]
    ].applymap(helper)

    if create_csv:
        df.to_csv(
            "./transactionData.csv",
            index=False,
            columns=[
                "doc_id",
                "ticker",
                "company",
                "asset",
                "type",
                "date",
                "amount_range",
                "description",
            ],
        )

    return df


def extractData(df, create_csv=False):
    results = []

    columns = ["doc_id", "company", "type", "date", "amount_range", "description"]
    num_rows = len(df)
    i = 1

    args = [[year, doc_id] for year, doc_id in zip(df["date"], df["doc_id"])]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for data in executor.map(getTransactionData, args):
            print(
                f"Current document processing: ID {args[i-1][1]} which is row {i} out of {num_rows}"
            )
            results.extend(data)
            i += 1

    if create_csv:
        with open("transactionsData.csv", "w", newline="") as file:
            writer = DictWriter(file, fieldnames=columns)
            writer.writeheader()
            for entry in results:
                writer.writerow(entry)

    df = pd.DataFrame(results, columns=columns)

    df = capitalize(
        extractAssetType(
            updateName(createTickerColumn(cleanName(cleanDescription(df))))
        )
    )

    return df
