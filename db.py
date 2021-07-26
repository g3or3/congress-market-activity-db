import sqlite3


def connectDb(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as error:
        print(error)

    return conn


def createTables(conn):
    c = conn.cursor()

    create_record_table = """CREATE TABLE IF NOT EXISTS record (
                                    id integer PRIMARY KEY,
                                    doc_id text,
                                    ticker text,
                                    company text NOT NULL,
                                    asset text,
                                    type text NOT NULL,
                                    date text NOT NULL,
                                    amount_range text NOT NULL,
                                    description text
                                );"""

    create_person_table = """CREATE TABLE IF NOT EXISTS person (
                                    doc_id text PRIMARY KEY,
                                    first_name text NOT NULL,
                                    last_name text NOT NULL,
                                    url text NOT NULL,
                                    FOREIGN KEY (doc_id) REFERENCES record (doc_id) 
                                );"""
    try:
        c.execute(create_record_table)
        c.execute(create_person_table)

    except sqlite3.Error as error:
        print(error)

    finally:
        c.close()