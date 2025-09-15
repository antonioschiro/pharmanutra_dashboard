import os
from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
#from airflow.utils.dates import days_ago
#from datetime import datetime
import yfinance as yf
import pandas as pd
from pytrends.request import TrendReq
from pendulum import datetime, duration, now
import time

POSTGRES_CONN_ID = os.environ.get("DATABASE_CONN_ID")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 8, 21),
    "retries": 3,
    "retry_delay": duration(minutes = 5),
}

with DAG(dag_id = "pharmanutra_pipeline",
         default_args = default_args,
         schedule = duration(hours = 6),
         catchup = False) as dag:

    with TaskGroup(group_id = "stock_flow") as stock_flow:
        @task()
        def extract_stock_values() -> pd.DataFrame:
            ticker = yf.Ticker("PHN.MI")
            stock_df = ticker.history(period="1d", interval="1d")
            stock_df.reset_index(inplace = True, names = "date")
            return stock_df
    
        @task()
        def transform_stock_df(stock_df: pd.DataFrame) -> pd.DataFrame:
            col_names = {col: col.strip().replace(" ", "_").lower() for col in stock_df.columns}
            stock_df.rename(columns = col_names, inplace = True)
            stock_df["date"] = pd.to_datetime(stock_df["date"])
            stock_df["date"] = stock_df["date"].apply(lambda x: x.date())
            return stock_df
        
        @task()
        def load_stock(stock_df: pd.DataFrame) -> None:
            # Initialize connection
            pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
            
            with pg_hook.get_conn() as conn:
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

        # Flow definition for taskgroup.
        stock_df = extract_stock_values()
        stock_df_processed = transform_stock_df(stock_df)
        load_stock(stock_df_processed)

    with TaskGroup(group_id = "kws_flow") as kws_flow:
        @task()
        def extract_research_trend() -> pd.DataFrame:
            kw_list = ["cetilar", "ultramag", "sideral", "apportal"]

            # using PROXY to avoid undesired rate limits.
            from proxy.check_proxy import proxy_list

            # Timeframe definition
            upper_date = now().date()
            lower_date = now().date() - duration(days = 1)
            timeframe = "{lower} {upper}".format(lower = lower_date, upper = upper_date)

            for url in proxy_list:
                try:
                    requests_args={'proxies': {'http': url}}
                    trend_getter = TrendReq(hl = "it-IT",
                                        tz = 120,
                                        timeout = (15,30),
                                        requests_args = requests_args,
                                    )
                    trend_getter.build_payload(kw_list = kw_list, timeframe= timeframe, geo = 'IT')
                    kw_trend_df = trend_getter.interest_over_time()
                    return kw_trend_df
                except Exception as e:
                    print(f"Proxy {url} failed. Exception: {e}")
                    time.sleep(1.0)
                    continue
            raise RuntimeError("All proxies failed.")
                    
    
        @task()
        def transform_kw_df(trend_df: pd.DataFrame) -> pd.DataFrame:
            # Manage index and columns names
            trend_df.index.rename(name = "date", inplace = True)
            col_names = {col: col.strip().replace(" ", "_").lower() for col in trend_df.columns}
            trend_df.rename(columns = col_names, inplace = True)
            # Transform dataframe to handle safely future additional keyword
            df_dict = { 
                "date": [],
                "keyword": [],
                "research_amount": [],
                "is_partial": [],
                }
            for date, value in trend_df.iterrows():
                for i in range(len(value)):
                    if i == len(value) - 1: # This IF statement is needed to replicate "is_partial" value for each keyword.
                        for _ in range(len(value) -1):
                            df_dict["is_partial"].append(value.iloc[i])
                    else:
                        df_dict["date"].append(date)
                        df_dict["keyword"].append(value.index[i])
                        df_dict["research_amount"].append(value.iloc[i])
            kw_df = pd.DataFrame(df_dict)
            return kw_df
    
        @task()
        def load_trend(trend_df: pd.DataFrame) -> None:
            # Initialize connection
            pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cursor:  
                    for _, row in trend_df.iterrows():
                        cursor.execute(f"""INSERT INTO kw_trend
                                    (kw_date, keyword, daily_search_amount, is_partial)
                                    VALUES ('{row["date"]}',
                                            '{row["keyword"]}', 
                                            {row["research_amount"]},
                                            {row["is_partial"]}
                                            )
                                    ON CONFLICT (kw_date, keyword)
                                    DO UPDATE SET daily_search_amount = {row["research_amount"]},
                                                is_partial = {row["is_partial"]};
                                    """)
            conn.close()

        # Flow definition for taskgroup.
        trend_df = extract_research_trend()
        trend_df_processed = transform_kw_df(trend_df)
        load_trend(trend_df_processed)