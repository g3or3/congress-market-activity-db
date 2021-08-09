from dotenv import load_dotenv
from sqlalchemy import create_engine
import os


def connectDb():
    load_dotenv()
    db_url = os.getenv("DEV_DATABASE")
    db = create_engine(db_url)

    return db


def createTables(db):
    create_record_table = """CREATE TABLE IF NOT EXISTS record (
                                    record_id serial PRIMARY KEY,
                                    doc_id text,
                                    ticker text,
                                    company text NOT NULL,
                                    asset text,
                                    type text NOT NULL,
                                    date date NOT NULL,
                                    amount_range text NOT NULL,
                                    description text,
                                    FOREIGN KEY (doc_id) REFERENCES person_to_record (doc_id)
                                );"""

    create_person_table = """ CREATE SEQUENCE person_id_seq;
                              CREATE TABLE IF NOT EXISTS person (
                                    person_id integer NOT NULL DEFAULT nextval('person_id_seq'),
                                    first_name text NOT NULL,
                                    last_name text NOT NULL,
                                    PRIMARY KEY (person_id)
                                );"""

    create_relationship_table = """CREATE TABLE IF NOT EXISTS person_to_record (
                                    doc_id text PRIMARY KEY,
                                    person_id integer NOT NULL,
                                    url text NOT NULL,
                                    FOREIGN KEY (person_id) REFERENCES person (person_id)
                                );"""

    db.execute(create_person_table)
    db.execute(create_relationship_table)
    db.execute(create_record_table)
