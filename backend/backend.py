import os
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2 as pg
from contextlib import asynccontextmanager
from pendulum import date, duration, now
import uvicorn
from utils import (
                jsonstring_with_date, 
                process_stock_stat, 
                process_kw_data,
                process_kw_stat, 
                sql_to_dict,
            )

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

CLIENT_URL = os.environ.get("CLIENT_URL")

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        conn = pg.connect(**credentials)
        with conn.cursor() as cursor:
            cursor.execute(f"""SELECT column_name
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = 'stock_trend'""")
            stock_cols = [t[0] for t in cursor.fetchall()[1:6]]
            cursor.execute(f"""SELECT column_name
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = 'kw_trend'""")
            kw_cols = [t[0] for t in cursor.fetchall()[1:]]
        app.state.conn = conn
        app.state.stock_cols = stock_cols
        app.state.kw_cols = kw_cols
        yield
    except pg.Error:
        raise pg.Error
    finally:
        conn.close()

app = FastAPI(lifespan = lifespan)

origins_regex = CLIENT_URL + r":\d+"
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex = origins_regex,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/stock")
def get_stock_values(start_date: None|str = None, end_date: None|str = None) -> str:
    conn = app.state.conn
    col_names = app.state.stock_cols
    # Set time range
    if start_date is None:
        start_date = now().date() - duration(years = 1)
    if end_date is None:
        end_date = now().date()
    query = """SELECT stock_date, open, high, low, close
                FROM stock_trend
                where stock_date BETWEEN %(start)s AND %(end)s;
                """
    try:
        with conn.cursor() as cursor:
            # Execute query
            cursor.execute(query, {"start": start_date,
                                   "end": end_date, 
                                })
            response = cursor.fetchall()
        # Convert result into jsonstring
        stock_dict = sql_to_dict(query_result = response, col_names = col_names)
        stock_json = jsonstring_with_date(stock_dict)
    except pg.errors.Error as sql_error:
        raise HTTPException(status_code = 400, detail = sql_error.pgerror)
    return stock_json

@app.get("/stock/stat")
def get_stock_stats():
    conn = app.state.conn
    query = """WITH lagged_stock AS (                
        select stock_date, stock_trend.open,
        LEAD(stock_trend.open, 1)
        OVER (order by stock_date desc) previous_open,
        stock_trend.close,
        LEAD(stock_trend.close, 1)
        OVER (order by stock_date desc) previous_close,
        stock_trend.low,
        LEAD(stock_trend.low, 1)
        OVER (order by stock_date desc) previous_low,
        stock_trend.high,
        LEAD(stock_trend.high, 1)
        OVER (order by stock_date desc) previous_high
        FROM stock_trend
        limit 1
        )
        SELECT stock_date,
        open,
        ((open - previous_open)/previous_open)*100 as open_percentage_lag,
        close,
        ((close - previous_close)/previous_close)*100 as close_percentage_lag,
        low,
        ((low - previous_low)/previous_low)*100 as low_percentage_lag,
        high,
        ((high - previous_high)/previous_high)*100 as high_percentage_lag
        FROM lagged_stock;
    """
    try:
        with conn.cursor() as cursor:
            # Execute query
            cursor.execute(query)
            response = cursor.fetchall()
            stats_dict = process_stock_stat(response)
            stats_json = jsonstring_with_date(stats_dict)
    except pg.errors.Error as sql_error:
        raise HTTPException(status_code = 400, detail = sql_error.pgerror)
    return stats_json

@app.get("/keyword")
def get_keyword_searches(start_date: None|str = None, end_date: None|str = None):
    conn = app.state.conn
    col_names = app.state.kw_cols    
    if start_date is None:
        start_date = now().date() - duration(years = 1)
    if end_date is None:
        end_date = now().date()
    query = """SELECT kw_date, keyword, daily_search_amount, is_partial
                FROM kw_trend
                where kw_date BETWEEN %(start)s AND %(end)s
                ORDER BY keyword ASC, kw_date ASC;
                """
    try:
        with conn.cursor() as cursor:
            # Execute query
            cursor.execute(query, {"start": start_date,
                                    "end": end_date, 
                                })
            response = cursor.fetchall()
        # Convert result into jsonstring
        kw_dict = sql_to_dict(query_result = response, col_names = col_names)
        kw_dict_processed = process_kw_data(kw_dict)
        kw_json = jsonstring_with_date(kw_dict_processed)
    except pg.errors.Error as sql_error:
        raise HTTPException(status_code = 400, detail = sql_error.pgerror)
    return kw_json

@app.get("/keyword/stat")
def get_kw_stats():
    conn = app.state.conn
    query = """ select kw_date, keyword, daily_search_amount,
        LEAD( daily_search_amount, 1)
        OVER ( partition by keyword
                    order by kw_date desc) lagged_amount
        FROM kw_trend
        ORDER BY kw_date desc, keyword asc
        LIMIT (select count(distinct keyword) from kw_trend) 
    """
    try:
        with conn.cursor() as cursor:
            # Execute query
            cursor.execute(query)
            response = cursor.fetchall()
            stats_dict = process_kw_stat(response)
            stats_json = jsonstring_with_date(stats_dict)
    except pg.errors.Error as sql_error:
        raise HTTPException(status_code = 400, detail = sql_error.pgerror)
    return stats_json

if __name__ == "__main__":
    config = uvicorn.Config("backend:app", port = 7000, log_level = "info", reload = True)
    server = uvicorn.Server(config)
    server.run()