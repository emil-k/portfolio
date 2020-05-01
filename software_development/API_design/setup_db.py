import pandas as pd
import datetime
from sqlalchemy import create_engine

def read_df():
    path = "dataset.csv"
    df = pd.read_csv(path)
    df.date = df.date.apply(lambda x: datetime.date(int(x.split("-")[0]),
                                                    int(x.split("-")[1]),
                                                    int(x.split("-")[2])))
    return df

def insert_data_to_db(df):
    engine = create_engine('postgresql://postgres:YOURPASSWORD@localhost:5432/my_db')
    df.to_sql('stats', engine)

def setup():
    df = read_df()
    insert_data_to_db(df)
