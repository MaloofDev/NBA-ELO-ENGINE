import sqlite3
import pandas as pd
import os
from datetime import datetime
import NBA_Tools as nba 

# Get the path to the database
current_dir = os.path.dirname(__file__)  # Directory of the current script
nbadb = os.path.join(current_dir, '..', 'Data', 'NBA.db')

# Get players from game 

con = sqlite3.connect(nbadb)
cur = con.cursor()

games = pd.read_sql('SELECT * FROM CLEANGAMES ', con)
rosters = pd.read_sql('SELECT * FROM ROSTERS ', con)
players = pd.read_sql('SELECT * FROM PLAYERS ', con)

# Initialize ELO for each player
players['ELO'] = 1000

# Calculate Team Elos 
    # EloA = Sum(Roster[i].Elo)
    # EloB = Sum(Roster[i].Elo)
# Calculate both teams expected win prob
    # Expected win prob Team A  E(A) = 1 / (1 + 10 ^ ((EloA - EloB) / 400))
    # Expected win prob Team B  E(B) = 1 / (1 + 10 ^ ((EloB - EloA) / 400))
# Calculate Change in Elo For Teams 
    # ELoA' = EloA + K * (Outcome - Expected)
    # ELoB' = EloB + K * (Outcome - Expected)
    # NewTeamEloA = EloA' + EloA
    # NewTeamEloA = EloB' + EloB
# Calculate Change in Elo For Players
    # PlayerElo' = NewTeamEloA * (PlayerElo / EloA)
# Update Player Elo
    # PlayerElo = PlayerElo' + PlayerElo

def calc_elo(game):

    home = nba.get_Home(game)
    visitor = nba.get_Visitor(game)

    home_elo = 0
    visitor_elo = 0

    # Rating = R + K * (Outcome - Entry)
    # 


print(players)

#for index, game in games.iterrows():
    #calc_elo(game)