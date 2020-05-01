import sys
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import pandas as pd
import time


class AWS_bucket:
    def __init__(self, bucket_name, start_year=2018, end_year=2020, min_obj_size=137):
        self.s3 = boto3.resource('s3', config=Config(signature_version=UNSIGNED))
        self.bucket = self.fetch_bucket(bucket_name)
        self.objects = self.fetch_objects()
        self.filtered_objects = self.filter_objects(start_year, end_year, min_obj_size)

    def fetch_bucket(self, bucket_name):
        '''
        :param bucket_name: (str) S3 bucket name
        :return: boto3.resources.factory.s3.Bucket
        '''
        my_bucket = self.s3.Bucket(bucket_name)
        return my_bucket

    def fetch_objects(self):
        '''
        :return: boto3.resources.collection.s3.Bucket.objectsCollection
        '''
        objects = self.bucket.objects.all()
        return objects

    def filter_objects(self, start_year, end_year, min_obj_size):
        '''
        :param start_year: (int)
        :param end_year: (int)
        :param min_obj_size: (int)
        :return: (list) filtered list of s3 bucket objects
        '''
        objects = [obj for obj in self.objects if obj.size > min_obj_size
                   and int(obj.key[:4]) in range(start_year, end_year+1)]
        assert len(objects) > 0
        return objects

    def get_keys(self):
        '''
        Creates a generator of all filenames (strings) in the filtered objects.
        :return: (generator)
        '''
        return (obj.key for obj in self.filtered_objects)

    def get_dir_structure(self):
        '''
        All files come in a mixed bag, and I need them grouped by day.
        :return: dict,
        key: unique date
        value: list of filenames
        '''
        from collections import defaultdict
        day_dict = defaultdict(list)
        for key in self.get_keys():
            day_dict[key.split("/")[0]].append(key)
        return day_dict

class Report:
    def __init__(self, aws_bucket, ISIN):
        self.ISIN = ISIN
        self.aws_bucket = aws_bucket
        self.directory_dict = self.aws_bucket.get_dir_structure()

    def create_daily_summary(self):
        '''
        Read all hour-level files for specific day and merge them into one dataframe on day level.
        Transform it into a daily summary of len 1.
        Append this summary to a list of summaries.
        Turn this list into an informative summary dataframe.
        :return: pd.DataFrame
        A df of daily stock performance summaries, one row corresponds to one day.
        '''
        alles = []

        for day in self.directory_dict:
            day_df = pd.DataFrame()
            for hour_file in self.directory_dict[day]:
                one_hour_df = pd.read_csv(f's3://deutsche-boerse-xetra-pds/{hour_file}')
                one_hour_df = one_hour_df[one_hour_df['ISIN'] == self.ISIN]
                if not one_hour_df.empty:
                    day_df = day_df.append(one_hour_df)
            if not day_df.empty:
                day_df_summary = self.extract_info_df_daily(day_df)
                assert len(day_df_summary) > 0
                alles.append(day_df_summary)
        final_output =  self.transform_daily_summary(alles)
        return final_output

    def transform_daily_summary(self, day_list):
        '''
        Takes a list of daily performance metrics, calculates new metrics, outputs a dataframe ready for writing to disc.
        :param day_list: (list), list of daily performance summaries
        :return: (pd.DataFrame), df with additional metrics calculated, ready for output.
        '''
        df = pd.DataFrame(day_list, columns=['date', 'open', 'close', 'volume'])
        df['closing_day_before'] = df['close'].shift(1)
        df['%_change_close'] = df.close / df.closing_day_before
        df['%_change_close'] = df['%_change_close'] - 1
        return df

    def extract_info_df_daily(self, df):
        '''
        Turns a minute-level data into a daily summary of one row.
        :param df: (pd.DataFrame), on minute-level
        :return: (list), daily summary of performance
        '''
        # For dev purposes. Replace assertions with custom exceptions in prod.
        assert len(df.Date.unique()) == 1
        date = df.Date.unique()[0]
        df['dt'] = pd.to_datetime(df['Date'] + " "+ df['Time'])
        opening = df[df.dt == min(df.dt)]['StartPrice'].tolist()[0]
        closing = df[df.dt == max(df.dt)]['EndPrice'].tolist()[0]
        daily_traded_volume = df.TradedVolume.sum()
        return [date, opening, closing, daily_traded_volume]

def main(ISIN):
    bucket = AWS_bucket('bucket_name')
    report = Report(bucket, ISIN)
    final_output = report.create_daily_summary()
    final_output.to_csv(f"summary_{ISIN}.csv")


if __name__ == "__main__":
    start_time = time.time()
    args = sys.argv[1:]
    if not args or len(args) > 1:
        raise Exception("Expected 1 cmd line argument")
    ISIN = args[0]
    if type(ISIN) != str:
        raise TypeError("ISIN must be a string")
    main(ISIN)
    print("time elapsed: {:.2f}s".format(time.time() - start_time))
