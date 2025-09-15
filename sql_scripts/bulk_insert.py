import os
from dotenv import load_dotenv
import yfinance as yf
import pandas as pd
from pytrends.request import TrendReq
from pendulum import datetime, duration, now, Date
import psycopg2 as pg

# load environment variables
load_dotenv()

# Load PostgreSQL credentials.
credentials = {
    "database": os.environ.get("DATABASE"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("HOST"),
    "port": os.environ.get("PORT"),
}

# INSERT ROWS IN STOCK_TREND TABLE
PERIOD = "30d" # example of possible values: "2y"

def extract_stock_values() -> pd.DataFrame:
    ticker = yf.Ticker("PHN.MI")
    stock_df = ticker.history(period= PERIOD, interval="1d")
    stock_df.reset_index(inplace = True, names = "date")
    return stock_df

def transform_stock_df(stock_df: pd.DataFrame) -> pd.DataFrame:
    col_names = {col: col.strip().replace(" ", "_").lower() for col in stock_df.columns}
    stock_df.rename(columns = col_names, inplace = True)
    stock_df["date"] = pd.to_datetime(stock_df["date"])
    stock_df["date"] = stock_df["date"].apply(lambda x: x.date())
    return stock_df

def load_stock(stock_df: pd.DataFrame) -> None:
    try:
        with pg.connect(**credentials) as conn:
            with conn.cursor() as cursor:
                for _, row in stock_df.iterrows():
                    cursor.execute(f"""INSERT INTO stock_trend
                                        (stock_date, open, high, low, close, volume, dividends, stock_splits)
                                        VALUES ('{row["date"]}',
                                                    {row["open"]}, 
                                                    {row["high"]},
                                                    {row["low"]},
                                                    {row["close"]},
                                                    {row["volume"]},
                                                    {row["dividends"]},
                                                    {row["stock_splits"]}
                                                    )
                                        ON CONFLICT (stock_date)
                                        DO UPDATE SET open = {row["open"]},
                                                    high = {row["high"]},
                                                    low = {row["low"]},
                                                    close = {row["close"]},
                                                    volume = {row["volume"]},
                                                    dividends = {row["dividends"]},
                                                    stock_splits = {row["stock_splits"]};
                                    """)
        conn.close()
    except (pg.Error, Exception) as exception:
        raise exception

def stock_bulk_pipe():
    """ETL pipeline for stock values bulk insert in postgreSQL db."""
    stock_df = extract_stock_values()
    print(f"Successfully downloaded {stock_df.shape[0]} rows.")
    stock_df_processed = transform_stock_df(stock_df)
    print("Dataframe processed.")
    load_stock(stock_df_processed)

# INSERT ROWS IN KW_TREND TABLE
UPPER_DATE = now().date()
LOWER_DATE = UPPER_DATE

#LOWER_DATE = Date(year = 2025, month = 8, day = 27)
#UPPER_DATE =  Date(year = 2025, month = 9, day = 2)

def split_daterange(lower: Date = LOWER_DATE, upper: Date = UPPER_DATE, step = 90) -> list[list]:
    """
    This function creates a list of 90 days date intervals. 
    This transformation is mandatory since Google trends does not return daily trends for intervals longer than 90 days.
    """
    interval = UPPER_DATE - LOWER_DATE
    if interval.as_duration().total_days() < 90:
        return [[LOWER_DATE, UPPER_DATE]]
    
    dates_list = []
    temp_date = lower
    temp_list = []
    while temp_date <= upper:
        # Update the temp_list.
        temp_list.append(temp_date)
        temp_date += duration(days = step)
        if temp_date > upper:
            temp_date = upper
        temp_list.append(temp_date)
        # Append it to dates_list.
        dates_list.append(temp_list)
        # Update value for temp_date to avoid date repetition.
        temp_date += duration(days = 1)
        if temp_date > upper:
            break
        # Clear temporary list.
        temp_list = []

    return dates_list

def extract_research_trend() -> pd.DataFrame:
    kw_list = ["cetilar", "ultramag", "sideral", "apportal"]
    complete_df = None
    date_ranges = split_daterange()

    # using PROXY to avoid undesired rate limits.
    from proxy.check_proxy import proxy_list
    requests_args={'proxies': {'http': proxy_list}}

    for l,u in date_ranges:
        timeframe = "{lower} {upper}".format(lower = l, upper = u)
        trend_getter = TrendReq(hl = "it-IT",
                                tz = 120,
                                timeout=(15,30),
                                requests_args= requests_args,
                            )
        trend_getter.build_payload(kw_list = kw_list, timeframe= timeframe, geo = 'IT')
        kw_trend_df = trend_getter.interest_over_time()
        if complete_df is None:
            complete_df = kw_trend_df
        else:
            complete_df = pd.concat([complete_df, kw_trend_df], axis = 0)
    print(complete_df)
    return complete_df

def transform_kw_df(trend_df: pd.DataFrame) -> pd.DataFrame:
    # Manage index and columns names
    trend_df.index.rename(name = "date", inplace = True)
    col_names = {col: col.strip().replace(" ", "_").lower() for col in trend_df.columns}
    trend_df.rename(columns = col_names, inplace = True)
    # Transform dataframe to handle safely future additional keyword
    df_dict = { 
        "kw_date": [],
        "keyword": [],
        "daily_search_amount": [],
        "is_partial": [],
        }
    for date, value in trend_df.iterrows():
        for i in range(len(value)):
            if i == len(value) - 1: # This IF statement is needed to replicate "is_partial" value for each keyword.
                for _ in range(len(value) -1):
                    df_dict["is_partial"].append(value.iloc[i])
            else:
                df_dict["kw_date"].append(date)
                df_dict["keyword"].append(value.index[i])
                df_dict["daily_search_amount"].append(value.iloc[i])
    kw_df = pd.DataFrame(df_dict)
    return kw_df

def load_trend(trend_df: pd.DataFrame) -> None:
    # Initialize connection
    try:
        with pg.connect(**credentials) as conn:
            with conn.cursor() as cursor:
                for _, row in trend_df.iterrows():
                    cursor.execute(f"""INSERT INTO kw_trend
                                (kw_date, keyword, daily_search_amount, is_partial)
                                VALUES ('{row["kw_date"]}',
                                        '{row["keyword"]}', 
                                        {row["daily_search_amount"]},
                                        {row["is_partial"]}
                                        )
                                ON CONFLICT (kw_date, keyword)
                                DO UPDATE SET daily_search_amount = {row["daily_search_amount"]},
                                            is_partial = {row["is_partial"]};
                                """
                                )
        conn.close()
    except pg.Error as error:
        raise error

def trend_bulk_pipe():
    """ETL pipeline for trend values bulk insert in postgreSQL db."""
    trend_df = extract_research_trend()
    print(f"Successfully downloaded {trend_df.shape[0]} rows.")
    trend_df_processed = transform_kw_df(trend_df)
    print("Dataframe processed.")
    load_trend(trend_df_processed)

if __name__ == "__main__":
    #stock_bulk_pipe()
    trend_bulk_pipe()