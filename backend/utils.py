from datetime import date
import json
from typing import Iterable
import numpy as np

def sql_to_dict(query_result: list[Iterable], col_names: Iterable[str]) -> dict:
    """
    Convert a list of table rows into a dictionary where each key is a column name and the corresponding value is a list of values for that column.
    Args:
        query_result (list[Iterable]): A list of rows, where each row is an iterable of column values.
        col_names (Iterable[str]): An iterable of column names corresponding to the columns in each row.
    Returns:
        dict: A dictionary mapping each column name to a list of its values from all rows.
    Raises:
        IndexError: If the number of columns in a row does not match the number of column names.
    """
    try:
        table_dict = {col: None for col in col_names}
        for index, key in enumerate(table_dict):
            table_dict[key] = [t[index] for t in query_result]
        return table_dict
    except IndexError as e:
        raise

def process_stock_stat(query_result: list[Iterable]) -> dict:
    stat_dict = {
        "stock_date": None,
        "open": None,
        "open_percentage": None,
        "close": None,
        "close_percentage": None,
        "low": None,
        "low_percentage": None,
        "high": None,
        "high_percentage": None
    }
    rows = query_result[0] # Extract the first (and only) tuple returned by query
    for value, key in zip(rows, stat_dict):
        if key == "stock_date":
            stat_dict[key] = value
        else:
            stat_dict[key] = np.round(value, 2)
    return stat_dict


def process_kw_data(kw_dict: dict) -> dict:
    """
    Processes a dictionary containing keywords trends data and restructures it by keyword.

    Args:
        kw_dict (dict): A dictionary with keys including 'keyword', 'kw_date', 'daily_search_amount', and 'is_partial'.
                        Each key maps to a list of values of equal length.

    Returns:
        dict: A dictionary where each keyword maps to a sub-dictionary containing lists for 'kw_date',
              'daily_search_amount', and 'is_partial'.

    Raises:
        IndexError: If the lists for any keyword do not have the same length.
    """
    cleaned_dict = { key: {"kw_date": [], "daily_search_amount": [], "is_partial": []} for key in set(kw_dict["keyword"])}
    for key in kw_dict:
        if key != "keyword":
            for index, val in enumerate(kw_dict[key]):
                if (cur_kw := kw_dict["keyword"][index]) in cleaned_dict:
                    cleaned_dict[cur_kw][key].append(val)  # This operation is valid since I used the same key values within cleaned_dict.

    def check_length(input_dict:dict) -> bool:
        reference_len = None
        for key in input_dict:
            for inner_key in (inner_dict := input_dict[key]):
                cur_inner_val = inner_dict[inner_key]
                if reference_len is None:
                    reference_len = len(cur_inner_val)
                if len(cur_inner_val) != reference_len:
                    raise IndexError(f"""One or more lists have different length.
                                    Error raised while checking this path: {key}.{inner_key}""")
        return True

    if check_length:
        return cleaned_dict

def process_kw_stat(query:list[Iterable]) -> dict:
    stat_dict = {}
    for row in query:
        cur_kw = row[1]
        stat_dict[cur_kw] = {
            "kw_date": row[0],
            "daily_search_amount": row[2],
            "lagged_amount": row[3], 
        }
    return stat_dict
                    
def jsonstring_with_date(python_dict: dict) -> str:
    """A wrapper that converts input object into JSON strings 
    handling date conversion trough serialize_date() function.
    """
    def serialize_date(obj):
            """Convert the date format into a ISO format string."""
            if isinstance(obj, date):
                return obj.isoformat()
            raise ValueError(f"Date object is not of type {date.__name__}.")
    json_string = json.dumps(python_dict, default= serialize_date)
    return json_string