import psycopg2
import pandas as pd
import re
import time
from sqlalchemy import create_engine

pd.set_option("max_rows", None)


def ask(question):
    while 1:
        print(question)
        s = input().lower()
        if s == 'y':
            return True
        elif s == 'n':
            return False
        else:
            print('You entered something wrong')


def get_last_excel():
    dt_index = pd.date_range('2020-12-01', pd.to_datetime('today'), freq='W-MON')
    dt_rev = [d.date() for d in reversed(dt_index)]
    for dt in dt_rev:
        try:
            path = f'call-center {dt} - {dt + pd.to_timedelta("6 days")}.xlsx'
            df_last = pd.read_excel(path)
            print(f'[INFO] File "{path}" is loaded')
            return df_last
        except Exception:
            pass


def get_specified_excel():
    while 1:
        print('Input the date, included in call-center excel report,\n'
              'in format "YYYY-mm-dd" (for example: 2021-01-01): ')
        s = input()
        try:
            date = pd.to_datetime(s)
            date = date - date.weekday() * pd.Timedelta('1 day')
            date = date.date()
            path = f'call-center {date} - {date + pd.to_timedelta("6 days")}.xlsx'
            df_spec = pd.read_excel(path)
            print(f'[INFO] File "{path}" is loaded')
            return df_spec
        except Exception as ex:
            print(ex)


def only_digits(text):
    res = ''
    text = str(text)
    for c in text:
        if c.isdigit():
            res += c
    return res


def is_phone_number(text):
    if re.match(r'[7-8]{1}[0-9]{9}', text) and len(text) == 11 \
            or re.match(r'[0-9]{9}', text) and len(text) == 10:
        return True
    else:
        return False


def to_standard_phone_number(text):
    text = only_digits(text)
    if not is_phone_number(text):
        return None
    if len(text) == 11:
        text = text[1:]
    return '7' + text


def transform_df(df):
    res = pd.DataFrame()
    res.insert(0, 'phone', df['АОН'].apply(to_standard_phone_number))
    res.insert(1, 'datetime', pd.to_datetime(df['Дата'] + ' ' + df['Время'], dayfirst=True))
    res.index.name = 'index'
    return res


def get_sql_query_text():
    start_year = df.iloc[0, 1].year
    end_year = df.iloc[-1, 1].year
    start_month = df.iloc[0, 1].month
    end_month = df.iloc[-1, 1].month
    if start_month < 10:
        start_month = '0' + str(start_month)
    if end_month < 10:
        end_month = '0' + str(end_month)
    sql_query = f'''
    SET SEARCH_PATH TO demo_calls;
    WITH get_call AS
        (
            SELECT index, datetime, min(datestart) AS min_date
            FROM from_excel
            INNER JOIN phone ON from_excel.phone = msisdn
            INNER JOIN subs_history sh ON phone.phone_id = sh.phone_id
            INNER JOIN call_{start_month}_{start_year} c ON sh.subs_id = c.subs_id
            WHERE datestart > datetime
            GROUP BY 1, 2
            UNION
            SELECT index, datetime, min(datestart)
            FROM from_excel
            INNER JOIN phone ON from_excel.phone = msisdn
            INNER JOIN subs_history sh ON phone.phone_id = sh.phone_id
            INNER JOIN call_{end_month}_{end_year} c ON sh.subs_id = c.subs_id
            WHERE datestart > datetime
            GROUP BY 1, 2
        )

    SELECT from_excel.phone, from_excel.datetime, min_date - from_excel.datetime AS internet_start_after
    FROM from_excel
    LEFT JOIN get_call ON from_excel.index = get_call.index
    ORDER BY 2; '''
    return sql_query


def while_ex():
    print('[INFO] Error while working with database via sqlalchemy engine:')
    print(ex)
    time.sleep(5)
    exit()

with open('config.txt', mode='r', encoding='utf-8-sig') as config:
    conf_str = ''
    for line in config:
        conf_str += line.strip() + ','
conf_str = "{" + conf_str + '}'
conf_dict = eval(conf_str)

with open('db_create.txt', mode='r', encoding='utf-8-sig') as db_create:
    db_create_str = db_create.read()
print('Welcome to call-center reporter tool!')
print('[INFO] Trying to connect to database...')

try:
    conn = psycopg2.connect(**conf_dict)
    with conn:
        with conn.cursor() as cur:
            cur.execute(db_create_str)
    print(f'[INFO] The schema "demo_calls" and the tables '
          f'have been successfully created in database "{conf_dict["database"]}"')
except Exception as ex:
    print('[INFO] Error while working with database via psycopg2:')
    print(ex)
    print('Check the parameters in config.txt and try again')
    time.sleep(5)
    exit()
finally:
    conn.close


if ask('Create report for last call-center excel file? (Y/N)'):
    df = get_last_excel()
else:
    df = get_specified_excel()
df = transform_df(df)

try:
    conn_alch = create_engine(f'postgresql+psycopg2://{conf_dict["user"]}:{conf_dict["password"]}'
                         f'@{conf_dict["host"]}:{conf_dict["port"]}/{conf_dict["database"]}')
    df.to_sql(name='from_excel', con=conn_alch, schema='demo_calls', if_exists='replace')
    print(f'[INFO] Support table \"{conf_dict["database"]}.demo_calls.from_excel\" has been uploaded')
except Exception as ex:
    while_ex()

try:
    df = pd.read_sql_query(get_sql_query_text(), con=conn_alch)
    print('[INFO] SQL query has been read')
except Exception as ex:
    while_ex()

print(df)
print('[INFO] The report has been created')
df['internet_start_after'] = df['internet_start_after'].astype('str')
if ask('Export to excel? (Y/N)'):
    path = f'Report {df.iloc[0, 1].date()} - {df.iloc[-1, 1].date()}.xlsx'
    df.to_excel(path,index=False)
    print(f'[INFO] File "{path}" has been saved')
print('Thank you for using call-center reporter tool!')
time.sleep(5)
exit()