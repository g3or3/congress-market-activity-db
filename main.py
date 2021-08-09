"""
Visits the following link:
"https://disclosures-clerk.house.gov/PublicDisclosure/FinancialDisclosure"
to download zip files containing text files of transaction data from congress
members. This data contains document IDs and filing dates which are parsed in
order to find the links to the PDFs that contain the actual transaction details.
"https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/<YEAR>/<DOCUMENT_ID>.pdf"
These PDFs are parsed and cleaned in order to collect relevant data before storing
in a postgres database for further use.

-> main.py is assuming you are starting the program from scratch <-

If you choose to create the CSV files you can use populateDbFromCsv.py
to create the database tables from the csv files instead.
"""

import sys
import db
import person
import transaction
import concurrent.futures
from partition import partition
from pandas import concat
from handleMissingDocs import addMissingData


def run(create_csv=False):
    begin_year = 2017
    end_year = 2021

    """ 
    Create the <YEAR> directories and download the appropriate text file.
    """

    try:
        person.downloadZipFiles(begin_year, end_year)

    except sys.exc_info()[0] as e:
        print(e)

    """
    Returns dataframe with person data including first name, last name,
    filing date (used to download the pdf later) and document id.
    """

    person_data, person_table, relationship_table = person.extractData(
        begin_year, end_year, create_csv
    )

    """
    Establish the connection to the database and populate the person table.
    Drops the data column when populating the table because that is only 
    necessary when making the request for the PDF file. 
    """

    db_engine = db.connectDb()

    try:
        db.createTables(db_engine)
    except sys.exc_info()[0] as e:
        print("Error creating tables.")

    try:
        person_table.to_sql("person", db_engine, index=False, if_exists="append")
        relationship_table.to_sql(
            "person_to_record", db_engine, index=False, if_exists="append"
        )
    except sys.exc_info()[0] as e:
        print(e)

    """
    Partitions the person dataframe into individual data frames to pass to 
    extractData() which will process each dataframe "concurrently" using multiple
    processes and threads. Each result will return a dataframe with transaction data 
    including doc_id, ticker, company, asset, type of transaction (purchase or sell), 
    date, amount_range and description to populate the record table.
    """

    args = partition(person_data[["date", "doc_id"]], 10)
    transaction_data = []

    with concurrent.futures.ProcessPoolExecutor() as executor:
        for result in executor.map(transaction.extractData, args):
            transaction_data.append(result)

    transaction_data = concat(transaction_data)

    if create_csv:
        transaction_data.to_csv("./data/transactionData.csv", index=False)

    try:
        transaction_data.to_sql("record", db_engine, index=False, if_exists="append")

    except sys.exc_info()[0] as e:
        print(e)


if __name__ == "__main__":
    run()
    addMissingData()
