import requests
import sqlite3
import pandas as pd
import sqlalchemy
import sqlite3
import time
import os
from datetime import datetime

# Get the path to the database
current_dir = os.path.dirname(__file__)  # Directory of the current script
nbadb = os.path.join(current_dir, '..', 'Data', 'NBA.db')

## Variables
reg_season_url = "https://www.basketball-reference.com/leagues/NBA_"
roster_url = "https://www.basketball-reference.com/teams/"
play_off_url = "https://www.basketball-reference.com/playoffs/NBA_"
months = ("october", "november", "december", "january", "february", "march", "april", "may", "june")

## Data Base Connection / Init
season_con = sqlite3.connect(nbadb)
season_cur = season_con.cursor()
season_cur.execute(''' CREATE TABLE IF NOT EXISTS GAMES
            ('Date' text,'Visitor' text, 'PTS' text, 'Home' text, 'PTS.1' text, PRIMARY KEY (Date, Visitor)) ''')
season_cur.execute(''' CREATE TABLE IF NOT EXISTS PlayerGames
            ('Player' text, 'GameID' text,  PRIMARY KEY (Player, GameID)) ''')

# Checks if url can be reached returns boolean
def is_valid_url(url):
     req = requests.get(url)
     return req.status_code == requests.codes['ok']

def valid_gamelog(url):

    try:
        pd.read_html(url, flavor='bs4')
        return True
    except ValueError:
        return False

## get_2020_games
## Issue with site formatting, hard-coded urls implemented
def get_2020_Games():
    
    urls = ['https://www.basketball-reference.com/leagues/NBA_2020_games-october-2019.html', 'https://www.basketball-reference.com/leagues/NBA_2020_games-november.html', 
            'https://www.basketball-reference.com/leagues/NBA_2020_games-december.html', 'https://www.basketball-reference.com/leagues/NBA_2020_games-january.html', 
            'https://www.basketball-reference.com/leagues/NBA_2020_games-february.html', 'https://www.basketball-reference.com/leagues/NBA_2020_games-march.html', 
            'https://www.basketball-reference.com/leagues/NBA_2020_games-july.html', 'https://www.basketball-reference.com/leagues/NBA_2020_games-august.html', 
            'https://www.basketball-reference.com/leagues/NBA_2020_games-september.html', 'https://www.basketball-reference.com/leagues/NBA_2020_games-october-2020.html', 
            'https://www.basketball-reference.com/playoffs/NBA_2020_games.html']
    
    for url in urls:
        
        # Grab list of tables from html as dataframe and select needed table 
            df = pd.read_html(url, flavor= "bs4")
            data = df[0]

            # Rename Columns to Match DB Table Structure
            data.rename(columns={'Visitor/Neutral': 'Visitor'}, inplace=True)
            data.rename(columns={'Home/Neutral': 'Home'}, inplace=True)

            # Select only needed columns from dataframe
            sort_data = data[['Date','Visitor', 'PTS', 'Home', 'PTS.1']]

            # Convert to list of tuples for sql commit
            sort_data = list(sort_data.itertuples(index= False, name= None))

            # Commit Data to DB
            season_cur.executemany("INSERT or IGNORE INTO GAMES VALUES (?, ?, ?, ?, ?)", sort_data)
            season_con.commit()

            time.sleep(5.5)

def Get_Games():

    year = 1950

    while year <= 2025:

        if year == 2020:
             get_2020_Games()
        else:

            for month in months:
                
                url = reg_season_url + str(year) + "_games-" + month +".html"

                if is_valid_url(url):
                    # Grab list of tables from html as dataframe and select needed table 
                    df = pd.read_html(url, flavor= "bs4")
                    data = df[0]

                    # Rename Columns to Match DB Table Structure
                    data.rename(columns={'Visitor/Neutral': 'Visitor'}, inplace=True)
                    data.rename(columns={'Home/Neutral': 'Home'}, inplace=True)

                    # Select only needed columns from dataframe
                    sort_data = data[['Date','Visitor', 'PTS', 'Home', 'PTS.1']]

                    # Convert to list of tuples for sql commit
                    sort_data = list(sort_data.itertuples(index= False, name= None))

                    # Commit Data to DB
                    season_cur.executemany("INSERT or IGNORE INTO GAMES VALUES (?, ?, ?, ?, ?)", sort_data)
                    season_con.commit()

                    # Sleep to avoid error 429 rate limiting
                    time.sleep(5.5)

            # Same Proccess as above just using playoff url
            url = play_off_url + str(year) + "_games.html"

            time.sleep(5.5)

            if is_valid_url(url):

                df = pd.read_html(url, flavor= "bs4")
                data = df[0]

                data.rename(columns={'Visitor/Neutral': 'Visitor'}, inplace=True)
                data.rename(columns={'Home/Neutral': 'Home'}, inplace=True)

                sort_data = data[['Date','Visitor', 'PTS', 'Home', 'PTS.1']]

                sort_data = list(sort_data.itertuples(index= False, name= None))

                season_cur.executemany("INSERT or IGNORE INTO GAMES VALUES (?, ?, ?, ?, ?)", sort_data)
                season_con.commit()

        year = year + 1

def clean_games():
    
    data_base = pd.read_sql('SELECT * FROM GAMES', season_con)
    no_none_data = data_base.mask(data_base.eq('None')).dropna()
    no_none_data = no_none_data[no_none_data.Visitor != "Playoffs"]

    season_cur.execute(''' CREATE TABLE IF NOT EXISTS CLEANGAMES
                ('Date' text,'Visitor' text, 'PTS' int, 'Home' text, 'PTS.1' int, 'GameID' text, PRIMARY KEY (Date, Visitor)) ''')
    
    no_none_data['Date'] = pd.to_datetime(no_none_data['Date'], format='%a, %b %d, %Y')
    
    no_none_data.sort_values(by=['Date'], inplace=True)

    no_none_data['Date'] = no_none_data['Date'].dt.strftime('%a, %b %d, %Y')

    no_none_data['GameID'] = range(1, len(no_none_data) + 1)

    no_none_data = list(no_none_data.itertuples(index= False, name= None))

    season_cur.executemany("INSERT or IGNORE INTO CLEANGAMES VALUES (?, ?, ?, ?, ?, ?)", no_none_data)
    season_cur.execute(''' DELETE FROM CLEANGAMES WHERE Visitor = "Playoffs" ''')
    season_con.commit()

def get_rosters():

    ABRS = ['AND', 'ATL', 'BLB', 'BOS', 'BRK', 'BUF', 'CAP', 'CHA', 'CHH', 'CHO', 
                'CHI','CHP', 'CHS', 'CHZ', 'CIN', 'CLE', 'DAL', 'DEN', 'DET', 
                'FTW', 'GSW', 'HOU', 'IND', 'INO', 'KCK', 'KCO', 'LAC', 'LAL', 'MEM', 
                'MIA', 'MIL', 'MLH', 'MNL', 'MIN', 'NJN', 'NOH', 'NOJ','NOP', 'NOK',
                'NYK', 'NYN', 'OKC', 'ORL','PHI', 'PHW', 'PHO', 'POR', 'ROC', 'SAC', 
                'SAS', 'SDC', 'SDR', 'SFW', 'SEA', 'SHE', 'STB', 'STL', 'SYR', 'TOR', 
                'TRI', 'UTA', 'VAN', 'WSB', 'WSC', 'WAS', 'WAT' ]
    
    season_cur.execute(''' CREATE TABLE IF NOT EXISTS ROSTERS
                ('Player' text,'Team' text, 'Year' int, PRIMARY KEY (Player, Team, Year)) ''')
    season_cur.execute(''' CREATE TABLE IF NOT EXISTS PLAYERS
                ('Player' text, PRIMARY KEY (Player)) ''')
    

    for abr in ABRS:

        year = 1950
        valid_url_count = 0

        while year <= 2025:
            
            url = roster_url + abr + "/" + str(year) + ".html"

            if is_valid_url(url):
                
                print(url)

                roster = (pd.read_html(url, flavor='bs4' ))[0]

                roster = roster[['Player']]

                season_cur.executemany("INSERT or IGNORE INTO PLAYERS VALUES (?)", list(roster.itertuples(index= False, name= None)))

                roster['Team'] = abr
                roster['Year'] = year

                season_cur.executemany("INSERT or IGNORE INTO ROSTERS VALUES (?, ?, ?)", list(roster.itertuples(index= False, name= None)))
                season_con.commit()
                time.sleep(5.5)
            
            year = year + 1
            url = roster_url + abr + "/" + str(year) + ".html"
            time.sleep(5.5)

def get_ABR(team_name, year):

    NBA_franchises = [
        ('AND', 'Anderson Packers'),
        ('ATL', 'Atlanta Hawks'),
        ('BLB', 'Baltimore Bullets'),
        ('BOS', 'Boston Celtics'),
        ('BRK', 'Brooklyn Nets'),
        ('BUF', 'Buffalo Braves'),
        ('CAP', 'Capital Bullets'),
        ('CHA', 'Charlotte Bobcats'),
        ('CHH', 'Charlotte Hornets'),  # Used only for 1989-2002
        ('CHO', 'Charlotte Hornets'),  # Used from 2015-present
        ('CHI', 'Chicago Bulls'),
        ('CHP', 'Chicago Packers'),
        ('CHS', 'Chicago Stags'),
        ('CHZ', 'Chicago Zephyrs'),
        ('CIN', 'Cincinnati Royals'),
        ('CLE', 'Cleveland Cavaliers'),
        ('DAL', 'Dallas Mavericks'),
        ('DEN', 'Denver Nuggets'),
        ('DET', 'Detroit Pistons'),
        ('FTW', 'Fort Wayne Pistons'),
        ('GSW', 'Golden State Warriors'),
        ('HOU', 'Houston Rockets'),
        ('IND', 'Indiana Pacers'),
        ('INO', 'Indianapolis Olympians'),
        ('KCK', 'Kansas City Kings'),
        ('KCO', 'Kansas City-Omaha Kings'),
        ('LAC', 'Los Angeles Clippers'),
        ('LAL', 'Los Angeles Lakers'),
        ('MEM', 'Memphis Grizzlies'),
        ('MIA', 'Miami Heat'),
        ('MIL', 'Milwaukee Bucks'),
        ('MLH', 'Milwaukee Hawks'),
        ('MNL', 'Minneapolis Lakers'),
        ('MIN', 'Minnesota Timberwolves'),
        ('NJN', 'New Jersey Nets'),
        ('NOH', 'New Orleans Hornets'),
        ('NOJ', 'New Orleans Jazz'),
        ('NOK', 'New Orleans/Oklahoma City Hornets'),
        ('NOP', 'New Orleans Pelicans'),
        ('NYK', 'New York Knicks'),
        ('NYN', 'New York Nets'),
        ('OKC', 'Oklahoma City Thunder'),
        ('ORL', 'Orlando Magic'),
        ('PHI', 'Philadelphia 76ers'),
        ('PHW', 'Philadelphia Warriors'),
        ('PHO', 'Phoenix Suns'),
        ('POR', 'Portland Trail Blazers'),
        ('ROC', 'Rochester Royals'),
        ('SAC', 'Sacramento Kings'),
        ('SAS', 'San Antonio Spurs'),
        ('SDC', 'San Diego Clippers'),
        ('SDR', 'San Diego Rockets'),
        ('SFW', 'San Francisco Warriors'),
        ('SEA', 'Seattle SuperSonics'),
        ('SHE', 'Sheboygan Red Skins'),
        ('STB', 'St. Louis Bombers'),
        ('STL', 'St. Louis Hawks'),
        ('SYR', 'Syracuse Nationals'),
        ('TOR', 'Toronto Raptors'),
        ('TRI', 'Tri-Cities Blackhawks'),
        ('UTA', 'Utah Jazz'),
        ('VAN', 'Vancouver Grizzlies'),
        ('WSB', 'Washington Bullets'),
        ('WSC', 'Washington Capitols'),
        ('WAS', 'Washington Wizards'),
        ('WAT', 'Waterloo Hawks')
    ]


    if team_name.lower() == 'denver nuggets':
        if year == 1950:
            return 'DNN'
    # Handle special case for Charlotte teams
    if team_name.lower() == 'charlotte hornets':
        if 1989 <= year <= 2002:
            return 'CHH'
        elif year >= 2015:
            return 'CHO'
    if team_name.lower() == 'charlotte bobcats':
        return 'CHA'

    # Search for the team name in the list
    for abbreviation, name in NBA_franchises:
        if name.lower() == team_name.lower():
            return abbreviation
    
    return team_name

def get_team_name(abr):
    
    NBA_franchises = [
        ('AND', 'Anderson Packers'),
        ('ATL', 'Atlanta Hawks'),
        ('BLB', 'Baltimore Bullets'),
        ('BOS', 'Boston Celtics'),
        ('BRK', 'Brooklyn Nets'),
        ('BUF', 'Buffalo Braves'),
        ('CAP', 'Capital Bullets'),
        ('CHA', 'Charlotte Bobcats'),
        ('CHH', 'Charlotte Hornets'),  # Used only for 1989-2002
        ('CHO', 'Charlotte Hornets'),  # Used from 2015-present
        ('CHI', 'Chicago Bulls'),
        ('CHP', 'Chicago Packers'),
        ('CHS', 'Chicago Stags'),
        ('CHZ', 'Chicago Zephyrs'),
        ('CIN', 'Cincinnati Royals'),
        ('CLE', 'Cleveland Cavaliers'),
        ('DAL', 'Dallas Mavericks'),
        ('DEN', 'Denver Nuggets'),
        ('DET', 'Detroit Pistons'),
        ('FTW', 'Fort Wayne Pistons'),
        ('GSW', 'Golden State Warriors'),
        ('HOU', 'Houston Rockets'),
        ('IND', 'Indiana Pacers'),
        ('INO', 'Indianapolis Olympians'),
        ('KCK', 'Kansas City Kings'),
        ('KCO', 'Kansas City-Omaha Kings'),
        ('LAC', 'Los Angeles Clippers'),
        ('LAL', 'Los Angeles Lakers'),
        ('MEM', 'Memphis Grizzlies'),
        ('MIA', 'Miami Heat'),
        ('MIL', 'Milwaukee Bucks'),
        ('MLH', 'Milwaukee Hawks'),
        ('MNL', 'Minneapolis Lakers'),
        ('MIN', 'Minnesota Timberwolves'),
        ('NJN', 'New Jersey Nets'),
        ('NOH', 'New Orleans Hornets'),
        ('NOJ', 'New Orleans Jazz'),
        ('NOK', 'New Orleans/Oklahoma City Hornets'),
        ('NOP', 'New Orleans Pelicans'),
        ('NYK', 'New York Knicks'),
        ('NYN', 'New York Nets'),
        ('OKC', 'Oklahoma City Thunder'),
        ('ORL', 'Orlando Magic'),
        ('PHI', 'Philadelphia 76ers'),
        ('PHW', 'Philadelphia Warriors'),
        ('PHO', 'Phoenix Suns'),
        ('POR', 'Portland Trail Blazers'),
        ('ROC', 'Rochester Royals'),
        ('SAC', 'Sacramento Kings'),
        ('SAS', 'San Antonio Spurs'),
        ('SDC', 'San Diego Clippers'),
        ('SDR', 'San Diego Rockets'),
        ('SFW', 'San Francisco Warriors'),
        ('SEA', 'Seattle SuperSonics'),
        ('SHE', 'Sheboygan Red Skins'),
        ('STB', 'St. Louis Bombers'),
        ('STL', 'St. Louis Hawks'),
        ('SYR', 'Syracuse Nationals'),
        ('TOR', 'Toronto Raptors'),
        ('TRI', 'Tri-Cities Blackhawks'),
        ('UTA', 'Utah Jazz'),
        ('VAN', 'Vancouver Grizzlies'),
        ('WSB', 'Washington Bullets'),
        ('WSC', 'Washington Capitols'),
        ('WAS', 'Washington Wizards'),
        ('WAT', 'Waterloo Hawks')
    ]

    for abbreviation, name in NBA_franchises:
        if abr == abbreviation:
            return name
    
    return abr

def get_Home(game):

    date = datetime.strptime(game['Date'], '%a, %b %d, %Y')
        
    if 10 <= date.month <= 12:
        year = date.year + 1
    else:
        year = datetime.strptime(game['Date'], '%a, %b %d, %Y').year

    home = get_ABR(game['Home'], year)

    query = f""" SELECT * FROM ROSTERS WHERE Team = '{home}' AND Year = {year} """
    roster = pd.read_sql(query, season_con)

    players = roster['Player'].tolist()

    return players

def get_Visitor(game):

    players = []

    date = datetime.strptime(game['Date'], '%a, %b %d, %Y')
        
    if 10 <= date.month <= 12:
        year = date.year + 1
    else:
        year = datetime.strptime(game['Date'], '%a, %b %d, %Y').year

    visitor = get_ABR(game['Visitor'], year)

    query = f""" SELECT * FROM ROSTERS WHERE Team = '{visitor}' AND Year = {year} """
    roster = pd.read_sql(query, season_con)

    players = roster['Player'].tolist()

    return players

def parse_date(date):

    parsed_date = datetime.strptime(date, "%a, %b %d, %Y")
    formatted_date = parsed_date.strftime("%Y-%m-%d")

    return formatted_date

def get_Player_Games(id):

    # Player Url Structure Url + First Letter Last + / + First 5 Last + First 2 First + 0X.html
    player_base_url = 'https://www.basketball-reference.com/players/'
    urls = []
    players = pd.read_sql('SELECT * FROM PlayerIds ', season_con)
    games = pd.read_sql('SELECT * FROM CLEANGAMES ', season_con)
    print(games)
    games['Date'] = games['Date'].apply(parse_date)
    print(games)

    players = players[players['PlayerID'].eq(id).cummax()]

    for player in players['Player']:

        query = f"SELECT * FROM ROSTERS WHERE Player = '{player.replace('\'', '\'\'')}'"
        rosters = pd.read_sql(query, season_con)
        first_year = rosters['Year'].min()
        last_year = rosters['Year'].max()
        name = player.replace(" ' ", "")
        first_name = name.split(' ')[0]
        last_name = name.split(' ')[1]
        index = 1
        playergameids = []


        for year in range(first_year, last_year + 1):

            url = player_base_url + last_name[0] + '/' + last_name[0:5] + first_name[0:2] + '0' + str(index) + '/gamelog/' + str(year)
            url = url.lower()
            print(url)

            while url in urls:
                index = index + 1
                url = player_base_url + last_name[0] + '/' + last_name[0:5] + first_name[0:2] + '0' + str(index) + '/gamelog/' + str(year)

            urls.append(url)

            if is_valid_url(url) & valid_gamelog(url):

                player_games = pd.read_html(url, flavor='bs4')
                formatted_tables = [i for i, df in enumerate(player_games) if 'Date' in df.columns]
                print(formatted_tables)

                for i in formatted_tables:
                    
                    table = player_games[i]
                    player_games = table[['Date', 'Tm']]

                    player_games['Tm'] = player_games['Tm'].apply(get_team_name)

                    gameids_visitor = games.merge(player_games, left_on=['Date', 'Visitor'], right_on=['Date', 'Tm'], how='inner')
                    gameids_home = games.merge(player_games, left_on=['Date', 'Home'], right_on=['Date', 'Tm'], how='inner')
                    gameids = pd.concat([gameids_visitor, gameids_home], ignore_index=True)
                    gameids = gameids['GameID'].tolist()

                    for gameid in gameids:
                        playergameids.append((player, gameid))
                    
                    season_cur.executemany("INSERT or IGNORE INTO PlayerGames VALUES (?, ?)", playergameids)
                    season_con.commit()

            time.sleep(5.5)


# url = 'https://www.basketball-reference.com/players/j/jamesle01/gamelog-playoffs/'

# games = pd.read_html(url, flavor='bs4')

# print(games)