#pylint: disable=all

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
#from reading_postgres_data import joining_tables
from ingame_predictions_calculation_live import in_game_predictions

def teams_xp_calculation(row) -> pd.DataFrame:
    """
    Calculate expected points (xP) for the home and away teams based on actions in a game.

    Parameters:
    -----------
    game_id : str
        Identifier for the game to calculate expected points.

    Returns:
    --------
    pd.DataFrame
        DataFrame containing calculated expected points (xP) for the home and away teams.

    Notes:
    ------
    This function performs the following steps:
    1. Read combined data from the function that combines data from PostgreSQL.
    2. Filters the DataFrame to include only rows corresponding to the specified game_id.
    3. Assigns expected point (xP) ratings to different actions based on predefined ratings.
    4. Calculates cumulative expected points (xP) for both home and away teams.
    5. Calculates the difference in scores and expected points between the home and away teams.
    6. Returns the DataFrame with calculated expected points and other relevant information.

    """

    xP_ratings = {'Restart':0.2, 'BallCarry':0.2,'Tackle': 0.5, 'Substitution':0.2, '22mRestart':0.3, 'Knockon':-0.2, 'Try':1.4,'ConversionKick':0.5, 'YellowCard':-1, 'ForwardPass':-0.3, 'RedCard':-1.4,
                'GoalLineRestart':0.3,'PenaltyGoalKick':0.2, 'DropKick':0.5, 'Conversion':1}

    home_team = row['team_name']
    away_team = row['opponent_name']



   
    if row['action'] in xP_ratings and row['team'] == home_team:
        row['home_team_xP'] = xP_ratings[row['action']]
    elif row['action'] in xP_ratings and row['team'] == away_team:
        row['away_team_xP'] = xP_ratings[row['action']]
    else:
        row['home_team_xP'] = 0
        row['away_team_xP'] = 0

    row['xp_diff'] = row['home_team_xP'] - row['away_team_xP']
   
    #calculate the difference between homeTeamScore and awayTeamScore
    row['score_diff'] = row['team_score_pbp'] - row['opposition_score_pbp']

    return row
