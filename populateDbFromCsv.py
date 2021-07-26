import db
import pandas as pd

conn = db.connectDb("transactions.db")

try:
    person_data = pd.read_csv("./personData.csv")
    transaction_data = pd.read_csv("./transactionData.csv")

    person_data.to_sql("person", conn, index=False, if_exists="replace")
    transaction_data.to_sql("record", conn, index=False, if_exists="replace")

except:
    print("Those csv files don't exist. Refer to main.py")

conn.close()
