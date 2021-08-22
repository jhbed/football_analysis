from football_analysis import DATABASE
import pandas as pd

def get_all_players():
    return pd.read_sql('select * from players', DATABASE)