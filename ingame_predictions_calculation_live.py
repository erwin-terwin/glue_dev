#pylint: disable=all

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import scipy.integrate as integrate
import scipy.stats as st
import math
from time import time
from tkinter import Misc
from tokenize import Exponent
import csv 



def coefficients():

    df_coefficients = pd.read_csv("coefficients_men.csv",header = None)

    sigmax_win =float(df_coefficients.iloc[0,1])
    sigmad_win = float(df_coefficients.iloc[1,1])
    rho_win = float(df_coefficients.iloc[2,1])

    a_win = df_coefficients.iloc[3,1].split(" ")
    a_win = [i.replace("[","") for i in a_win]
    a_win = [i.replace("]","") for i in a_win]
    a_win = [i.replace("\n","") for i in a_win]
    a_win = list(filter(None, a_win))
    a_win = [float(i) for i in a_win]

    c_win = df_coefficients.iloc[4,1].split(" ")
    c_win = [i.replace("[","") for i in c_win]
    c_win = [i.replace("]","") for i in c_win]
    c_win = [i.replace("\n","") for i in c_win]
    c_win = list(filter(None, c_win))
    c_win = [float(i) for i in c_win]

    sigmax_loss = float(df_coefficients.iloc[5,1])
    sigmad_loss = float(df_coefficients.iloc[6,1])
    rho_loss = float(df_coefficients.iloc[7,1])
    a_loss = df_coefficients.iloc[8,1].split(" ")
    a_loss = [i.replace("[","") for i in a_loss]
    a_loss = [i.replace("]","") for i in a_loss]
    a_loss = [i.replace("\n","") for i in a_loss]
    a_loss = list(filter(None, a_loss))
    a_loss = [float(i) for i in a_loss]

    c_loss = df_coefficients.iloc[9,1].split(" ")
    c_loss = [i.replace("[","") for i in c_loss]
    c_loss = [i.replace("]","") for i in c_loss]
    c_loss = [i.replace("\n","") for i in c_loss]
    c_loss = list(filter(None, c_loss))
    c_loss = [float(i) for i in c_loss]

    c_joint = df_coefficients.iloc[10,1].split(" ")
    c_joint = [i.replace("[","") for i in c_joint]
    c_joint = [i.replace("]","") for i in c_joint]
    c_joint = [i.replace("\n","") for i in c_joint]
    c_joint = list(filter(None, c_joint))
    c_joint = [float(i) for i in c_joint]
    sigmad_joint = float(df_coefficients.iloc[11,1])

    return sigmax_win,sigmad_win,rho_win,a_win,c_win,sigmax_loss,sigmad_loss,rho_loss,a_loss,c_loss,c_joint,sigmad_joint


def split_fda(score_diff,time,feature,sigmad,sigmax,rho,a,c):
    from bspline import Bspline
    import numpy as np

    knot_vector = [0,0,0,10,20,30,40,50,60,70,80,80,80]
    basis = Bspline(knot_vector,3)

    def basis_eval(x):
        basis_list = []
        for i in range(len(x)):
            basis_list.append(basis(x[i]))
        return basis_list

    def B(x):
        basis_list = basis_eval(x)
        B = np.array(basis_list)
        return B 

    list_1 = np.arange(0,81,5)

    D = score_diff
    X = feature
    BK = B(list_1)
    minutes = time
    g = int(np.round(minutes/5))

    if(minutes > 0):    
        bk = BK[g]
        c1 = 1/(2*math.pi*minutes*sigmax*sigmad*math.sqrt(1-rho**2))
        c2 = (X-np.dot(a,bk))**2/(minutes*sigmax**2)
        c3 = (2*rho*((X-np.dot(a,bk))*(D-np.dot(c,bk))))/(minutes*sigmax*sigmad)
        c4 = ((D-np.dot(c,bk))**2)/(minutes*sigmad**2)
        rho2 = -1/(2*(1-rho**2))
        fx = c1 * math.exp(rho2*(c2-c3+c4))
    else:
        fx = 1
        minutes = 0

    if fx == 0:
        fx = 1
    else:
        fx = fx 
    return fx, minutes


def joint_fda(score_diff,time,sigmad,c):
    from bspline import Bspline
    import numpy as np

    knot_vector = [0,0,0,10,20,30,40,50,60,70,80,80,80]
    basis = Bspline(knot_vector,3)

    def basis_eval(x):
        basis_list = []
        for i in range(len(x)):
            basis_list.append(basis(x[i]))
        return basis_list

    def B(x):
        basis_list = basis_eval(x)
        B = np.array(basis_list)
        return B

    list_1 = list(range(0,81,5))

    D = score_diff
    minutes = time
    BK = B(list_1)
    bk80 = B(list_1)[-1]
    g = int(np.round(minutes/5))

    if(minutes<80 and minutes >= 0):
        bk = BK[g]
        b = np.subtract(bk80,bk)
        fx = (D+np.dot(c,b))/(math.sqrt(80-minutes)*sigmad)
        prob = st.norm.cdf(fx)
    else:
        prob = 1
            
    return prob,minutes


def in_game_predictions(df) -> pd.DataFrame:

    """
    Calculate in-game win probabilities for a given game based on live game data and pre-game odds.

    Parameters:
    -----------
    game_id : str
        Identifier for the game to calculate in-game win probabilities.
    table_name : str
        Name of the table containing pre-game odds.

    Returns:
    --------
    pd.DataFrame
        DataFrame containing calculated in-game win probabilities.

    Notes:
    ------
    This function performs the following steps:
    1. Calls the `teams_xp_calculation()` function to calculate expected points (xP) for the game.
    2. Reads pre-game odds from the specified table using `read_pre_game_odds()` function.
    3. Extracts pre-game probability for the home team.
    4. Retrieves coefficients for the FDA (Functional Data Analysis) model using the `coefficients()` function.
    5. Iterates over each row of the DataFrame:
        - Determines the time elapsed in the game and calculates win probabilities using the FDA model.
        - Combines pre-game and in-game probabilities based on the current game time.
        - Appends the calculated weighted probability to the DataFrame.
    6. Inserts the pre-game probability at time 0 in the DataFrame.
    7. Sorts the DataFrame by game time.
    8. Returns the DataFrame with calculated in-game win probabilities.

    """

    po = 0.4

    sigmax_win,sigmad_win,rho_win,a_win,c_win,sigmax_loss,sigmad_loss,rho_loss,a_loss,c_loss,c_joint,sigmad_joint = coefficients()
    weighted_prob=1
    for index, row in df.iterrows():
        time = row['game_time']
        if( time >= 0 and time <= 40):
            time = row['game_time']
            score_diff = row['score_diff']
            feature = row['xp_diff_cum']
            win,mins = split_fda(score_diff,time,feature,sigmad_win,sigmax_win,rho_win,a_win,c_win)
            loss,mins = split_fda(score_diff,time,feature,sigmad_loss,sigmax_loss,rho_loss,a_loss,c_loss)
            p,min = joint_fda(score_diff,time,sigmad_joint,c_joint)
            probability = (win*po)/(win*po+loss*(1-po))
            #w = time/40
            weighted_prob = p

        elif(time >  40 and time < 80):
            time = row['game_time']
            score_diff = row['score_diff']
            feature = row['xp_diff_cum']
            win,mins = split_fda(score_diff,time,feature,sigmad_win,sigmax_win,rho_win,a_win,c_win)
            loss,mins = split_fda(score_diff,time,feature,sigmad_loss,sigmax_loss,rho_loss,a_loss,c_loss)
            p,min = joint_fda(score_diff,time,sigmad_joint,c_joint)
            probability = (win*po)/(win*po+loss*(1-po))
            w = (80 - time)/40
            weighted_prob = w*probability+(1-w)*p
        
        #append weighted_prob to df
        df.at[index, 'home_prob'] = np.round(weighted_prob,3)
        df.at[index, 'away_prob'] = np.round(1 - weighted_prob,3)

    #insert probaility = po and time = 0 in the first row of the dataframe
    # df = df.append({'game_time': 0, 'probability': po}, ignore_index=True)
    # df = df.sort_values(by = 'game_time')

    return df


