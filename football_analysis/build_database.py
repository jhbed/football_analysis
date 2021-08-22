'''
DANGER

This script is designed to build the database from scratch. So only run it if
you have backed up our db file somewhere and you are fine with the database
local to this git repo being destroyed.
'''
from football_analysis import DATABASE, query_util, util
import pandas as pd
import numpy as np
import sqlite3

YEARLY_DATA_DIR = "raw_data/yearly/"
DRAFT_RANKINGS = "raw_data/draft_rankings_2021.csv"

def build_year_stats_table():
    for i, year in enumerate(['2017', '2018','2019','2020']):
        table_action = 'replace' if i == 0 else 'append'
        filename = YEARLY_DATA_DIR + year + '.csv'
        df = pd.read_csv(filename)
        if 'Unnamed: 0' in df.columns:
            df.drop(columns=['Unnamed: 0'], inplace=True)
        df['year'] = year
        df = df.rename(columns={'index' : 'id', 'Player' : 'player_name'})
        df = util.add_player_id_to_player_based_table(df)
        df = util.lower_and_underscore_columns(df)
        print(table_action + "ing", "player_year_stats table")
        print(df.head())
        df.to_sql('player_year_stats', DATABASE, if_exists=table_action, index=False)

def build_root_players_table():
    # load the 2020 player file as our initial source of truth for player data.
    filename = YEARLY_DATA_DIR + '2020.csv'
    df = pd.read_csv(filename)
    df = df.reset_index()
    df = df.rename(columns={'index' : 'id', 'Player' : 'player_name'})
    rankings = pd.read_csv(DRAFT_RANKINGS)
    rankings = util.cleanse_ranking_df_colnames(rankings)
    rankings = util.add_player_id_to_player_based_table(
            rankings, source_of_truth_df=df, player_id_colname='id')
    rankings[['pos', 'pos_draft_rank']] = util.extract_position_and_rank_cols_from_column(rankings['pos_and_pos_rank'])
    df.id.max()
    print('Supposedly there are', rankings.id.max() - df.id.max(), 'rookies')
    print('there are', rankings.shape[0], 'players')
    print('OVERWRITING PLAYERS TABLE WITH THIS')
    print(rankings.head())
    rankings.to_sql('players', DATABASE, if_exists='replace', index=False)

if __name__ == '__main__':
    build_root_players_table()
    build_year_stats_table()



     




