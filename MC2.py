import pandas as pd
import csv


#read files
df1 = pd.read_csv('tx_canvass_data1.csv')
df2 = pd.read_csv('tx_canvass_data2.csv')

#clean up
def clean_up(df):
    df.columns = map(str.lower, df.columns)
    df.columns = [x.strip().replace('?', '') for x in df.columns]
    df.columns = [x.strip().replace('!', '') for x in df.columns]
    df.columns = [x.strip().replace('.', '') for x in df.columns]
    df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    return df

#reformat
df1 = clean_up(df1)
df2 = clean_up(df2)

#replace column headers in df2 before merge
df2.rename(columns={'person.id': 'person_id'}, inplace=True)
df2.rename(columns={'effort title':'effort_title'}, inplace=True)

#merge
full_df = pd.concat([df1, df2], sort=True)

#no duplicate columns
full_df = full_df.loc[:,~full_df.columns.duplicated()]

#return single csv file to upload
full_df.to_csv('merged_df.csv', index = False)

