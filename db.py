from dotenv import load_dotenv
import os
from sqlalchemy import create_engine


def connectDb():
    load_dotenv()
    db_url = os.getenv("DEV_DATABASE")
    db = create_engine(db_url)

    return db


def createTables(db):
    create_record_table = """CREATE TABLE IF NOT EXISTS record (
                                    id serial PRIMARY KEY,
                                    doc_id text,
                                    ticker text,
                                    company text NOT NULL,
                                    asset text,
                                    type text NOT NULL,
                                    date date NOT NULL,
                                    amount_range text NOT NULL,
                                    description text,
                                    FOREIGN KEY (doc_id) REFERENCES person (doc_id) 
                                );"""

    create_person_table = """CREATE TABLE IF NOT EXISTS person (
                                    doc_id text PRIMARY KEY,
                                    first_name text NOT NULL,
                                    last_name text NOT NULL,
                                    url text NOT NULL
                                );"""

    db.execute(create_person_table)
    db.execute(create_record_table)
