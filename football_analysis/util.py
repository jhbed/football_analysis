from football_analysis import DATABASE
from football_analysis.query_util import get_all_players
import pandas as pd
import numpy as np

def lower_and_underscore_columns(df):
    df = df.copy()
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
    return df

def cleanse_ranking_df_colnames(df):
    return df.reset_index().rename(columns={
        'index' : 'id',
        'RK' : 'draft_rank',
        'TIERS' : 'tier',
        'PLAYER NAME' : 'player_name',
        'TEAM' : 'team',
        'POS' : 'pos_and_pos_rank',
        'BYE WEEK' : 'bye_week',
        'SOS SEASON' : 'sos_season',
        'ECR VS. ADP' : 'ecr_vs_adp',
        'POSITION' : 'position',
        'POSITION_RANK' : 'position_draft_rank'
    })

def cleanse_player_name_col(col, split_first_and_last=False):
    # ORDER OF TOKENS MATTERS, DO NOT CHANGE THIS
    bad_tokens = [
        " jr.", # Note the space in front, just in case someones name is JR smith.
        " jr",
        " sr.",
        " sr",
        " iiii",
        " iii",
        " ii",
        ".",
        ",",
        "'", # for names like D'andre
        "-", # for names like Allie-cox
    ]
    col = col.copy()
    col = col.str.lower().str.strip()
    for token in bad_tokens:
        col = col.str.replace(token, '')
    col = col.str.strip()
    col = col.str.split(' ')
    return (pd.DataFrame({'first_name' : col.str[0], 'last_name' : col.str[-1]} )
            if split_first_and_last 
            else col.str[0] + ' ' + col.str[-1])



def add_player_id_to_player_based_table(df, player_name_colname='player_name', source_of_truth_df=None, player_id_colname='player_id'):
    df = df.copy()
    source_of_truth = source_of_truth_df.copy() if source_of_truth_df is not None else get_all_players()
    df[['first_name', 'last_name']] = cleanse_player_name_col(df[player_name_colname], split_first_and_last=True)
    df[player_id_colname] = -1
    df[player_id_colname] = df[player_id_colname].astype(int)
    source_of_truth[['first_name', 'last_name']] = cleanse_player_name_col(source_of_truth[player_name_colname], split_first_and_last=True)
    current_id = int(source_of_truth.id.max() + 1)
    for index, row in df.iterrows():
        id_matched = False
        for actual_id_idx, actual_id_row in source_of_truth.iterrows():
            first_name = actual_id_row.first_name
            last_name = actual_id_row.last_name
            actual_id = actual_id_row.id
            if ((first_name in row.first_name or row.first_name in first_name) and 
                    (last_name in row.last_name or row.last_name in last_name)):
                df.loc[index, player_id_colname] = int(actual_id)
                id_matched = True
        if not id_matched:
            df.loc[index, player_id_colname] = current_id
            current_id += 1
    return df

def extract_position_and_rank_from_rank_string(rank_string):
    for index, char in enumerate(rank_string):
        try:
            maybe_int = int(char)
            position = rank_string[0:index]
            rank = int(rank_string[index:])
            return position, rank
        except:
            pass

def extract_position_and_rank_cols_from_column(col):
    return col.apply(lambda col: pd.Series(
                                    {'pos' : extract_position_and_rank_from_rank_string(col)[0], 
                                     'rank' : extract_position_and_rank_from_rank_string(col)[1]}))