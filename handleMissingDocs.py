from dotenv import load_dotenv
from sqlalchemy import create_engine
import os
import pandas as pd
import transaction


def addMissingData():
    """
    Run after run() in main.py to re-run the script on missing document ids that
    extractData() did not catch the first time around.
    """

    load_dotenv()
    db_engine = create_engine(os.getenv("DEV_DATABASE"))

    relationship_table = pd.read_sql(
        """
    select * from person_to_record;
    """,
        db_engine,
    )

    record = pd.read_sql(
        """
    select * from record;
    """,
        db_engine,
    )

    tracked = set([_id for _id in record["doc_id"]])

    untracked = []

    for url, _id in zip(relationship_table["url"], relationship_table["doc_id"]):
        if not _id in tracked:
            untracked.append((url[-17:-13], _id))

    untracked = pd.DataFrame(untracked, columns=["date", "doc_id"])

    res = transaction.extractData(untracked)

    res.to_sql("record", db_engine, index=False, if_exists="append")
