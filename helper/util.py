import os
import re
import pandas as pd
from datetime import datetime

def get_name_as_date(filename):
    return datetime.strptime(filename.split('_')[1], '%b%y').strftime("%Y%m")

def files_df(folder="../../data/18-83510-I94-Data-2016/"):
    return pd.DataFrame(sorted([(get_name_as_date(f), folder, f'{f}') for f in os.listdir(folder)]), columns=["Date", "Folder", "File Name"])

def get_columns_descriptions(file="I94_SAS_Labels_Descriptions.SAS"):
    pattern = re.compile('\/\*[ \t]*|[ \t]*\*\/')
    #pattern.match(string)
    out = []
    temp = [re.sub(pattern,'', line.strip()) for line in open(file) if line.startswith('/*')]
    for i in temp:
        s = i.split(" - ")
        if len(s) >= 2:
            nested = s[0].split(" & ")
            if len(nested) == 2:
                for e in nested:
                    out.append((e, s[1]))
            else:
                out.append(tuple(s[:2]))
        else:
            s2 = i.split(" is the ")
            if len(s2) == 2:
                out.append(tuple(s2))
    return out

def files_df_shape(folder="../../data/18-83510-I94-Data-2016/"):
    out = []
    for f in os.listdir(folder):
        immigration_fname = f'{folder}{f}'
        df = pd.read_sas(immigration_fname, 'sas7bdat', encoding="ISO-8859-1")
        df.to_csv(f'{get_name_as_date(f)}.csv')
        rows, cols = df.shape
        out.append((get_name_as_date(f), folder, f'{f}', rows, cols))
    return pd.DataFrame(sorted(out), columns=["Date", "Folder", "File Name", "Rows", "Columns"])


def convert_sas_date(sas_date):
    if pd.isna(sas_date):
        return sas_date
    else:
        return pd.to_timedelta(sas_date, unit='D') + pd.datetime(1960, 1, 1)

def convert_integer(df, cols):
    return df.astype(dict(zip(cols, len(cols)*['Int64'])))
