import pandas as pd
import sqlite3
import transaction


def addMissingData():
    """
    Run after run() in main.py to re-run the script on missing document ids that
    extractData() did not catch the first time around.
    """

    conn = sqlite3.connect("./transactions.db")

    person = pd.read_sql(
        """
    select * from person;
    """,
        conn,
    )

    record = pd.read_sql(
        """
    select * from record;
    """,
        conn,
    )

    tracked = set([_id for _id in record["doc_id"]])

    untracked = []

    for url, _id in zip(person["url"], person["doc_id"]):
        if not _id in tracked:
            untracked.append((url[-17:-13], _id))

    untracked = pd.DataFrame(untracked, columns=["date", "doc_id"])

    res = transaction.extractData(untracked)

    res.to_sql("record", conn, index=False, if_exists="append")

    conn.close()
