import datetime
import glob
import json
import os
import pandas as pd
import requests
from typing import Dict, List, Optional


def request_json(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/90.0.4430.212 Safari/537.36'
    }
    r = requests.get(url, headers=headers)
    return r.json()


def date_today():
    return datetime.datetime.now().strftime('%Y-%m-%d')


def api_base():
    return 'https://web-cdn.api.bbci.co.uk/wc-poll-data/container'


def get_fixtures(start_date, end_date=None):
    if not end_date:
        end_date = start_date
    fixtures = f'{api_base()}/sport-data-scores-fixtures?selectedEndDate={end_date}&selectedStartDate={start_date}&todayDate={date_today()}&urn=urn%3Abbc%3Asportsdata%3Afootball%3Ateam%3Atranmere-rovers&useSdApi=false'
    return request_json(fixtures)['eventGroups']


def get_resource_id(fixture_info):
    return fixture_info['secondaryGroups'][0]['events'][0]['tipoTopicId']


def get_match_id(fixture_info):
    return fixture_info['secondaryGroups'][0]['events'][0]['id']


def get_match_stats(match_id):
    match_stats = f'{api_base()}/match-stats?globalContainerPolling=true&urn=urn%3Abbc%3Asportsdata%3Afootball%3Aevent%3A{match_id}'
    return request_json(match_stats)


def get_match_info(resource_id, game_date, match_id):
    match_info = f'{api_base()}/live-header?assetId={resource_id}&endDateTime={game_date}&globalContainerPolling=true&isInternational=true&liveExperienceCrowdCount=true&showMSI=false&showMedia=true&sportDataEventUrn=urn%3Abbc%3Asportsdata%3Afootball%3Aevent%3A{match_id}&sportDiscipline=football&startDateTime={game_date}&uasEnv=live'
    return request_json(match_info)


def get_sameday_fixtures(match_id):
    sameday_fixtures = f'{api_base()}/football-on-the-day-events?globalContainerPolling=true&matchUrn=urn%3Abbc%3Asportsdata%3Afootball%3Aevent%3A{match_id}'
    return request_json(sameday_fixtures)


def get_table(match_id, date = date_today()):
    table = f'{api_base()}/football-table?globalContainerPolling=true&matchDate={date}&matchUrn=urn%3Abbc%3Asportsdata%3Afootball%3Aevent%3A{match_id}'
    return request_json(table)


def get_lineups(match_id):
    line_ups = f'{api_base()}/match-lineups?globalContainerPolling=true&urn=urn%3Abbc%3Asportsdata%3Afootball%3Aevent%3A{match_id}'
    return request_json(line_ups)


def get_commentary_url(match_id, page_no):
    return f'https://www.bbc.com/wc-data/container/stream?globalContainerPolling=true&liveTextStreamId={match_id}&pageNumber={page_no}&pageSize=20&pageUrl=%2Fsport%2Ffootball%2Flive%2Fc0mn93jz28nt&type=football'


def get_commentary(match_id, page_no=1):
    commentary = get_commentary_url(match_id, page_no)

    all_commentary = []
    page_1 = request_json(commentary)
    if not 'error' in page_1.keys():
        all_commentary.append(page_1)
        
        n_pages = page_1['page']['total']
        for i in range(2, n_pages+1):
            commentary = get_commentary_url(match_id, i)
            all_commentary.append(request_json(commentary))

        return all_commentary
    

def get_match_json(game_date):
    fixture_info = get_fixtures(game_date)[0]

    bbc_resource_id = get_resource_id(fixture_info)

    bbc_match_id = get_match_id(fixture_info)

    match_stats = get_match_stats(bbc_match_id)

    match_info = get_match_info(bbc_resource_id, game_date, bbc_match_id)

    lineups = get_lineups(bbc_match_id)

    table = get_table(bbc_match_id)

    sameday_fixtures = get_sameday_fixtures(bbc_match_id)

    commentary = get_commentary(bbc_match_id)

    return {
        'fixture_info': fixture_info,
        'match_stats': match_stats,
        'match_info': match_info,
        'lineups': lineups,
        'table': table,
        'sameday_fixtures': sameday_fixtures,
        'commentary': commentary
    }

def save_match_json(game_date):
    bbc_json = get_match_json(game_date)

    print(f"~~~~~~ Saving data for {game_date} ~~~~~~")

    for key, value in bbc_json.items():
        dir = f"./bbc-json/{key}"
        os.makedirs(dir, exist_ok=True)
        
        filename = f"{dir}/{game_date}.json"
        
        with open(filename, 'w') as outfile:
            json.dump(value, outfile)
            print(f"Saved {filename}" )


def name_json_file(date: str) -> str:
    """ 
        Return the path to the JSON file for the given date. 
    """
    return f'./bbc-json/table/{date}.json'


def read_json_file(file: str) -> Dict:
    """ 
        Load and return the JSON data from a file. 
    """
    try:
        with open(file) as f:
            return json.load(f)
    except FileNotFoundError:
        raise Exception(f"File {file} not found.")
    except json.JSONDecodeError:
        raise Exception(f"Error decoding JSON from file {file}.")


def get_divs(data: Dict) -> List[Dict]:
    """ 
        Extract the division information from the JSON data. 
    """
    try:
        return data['tournaments'][0]['stages'][0]['rounds']
    except (KeyError, IndexError):
        raise Exception("Unexpected JSON structure for divisions.")


def find_team_div(divs: List[Dict], team_name: str) -> Optional[Dict]:
    """ 
        Find and return the division containing the specified team. 
    """
    for div in divs:
        teams = div.get('participants', [])
        if any(team.get('name') == team_name for team in teams):
            return div
    return None


def get_league_name(data: Dict) -> str:
    """ 
        Extract the league name.
    """
    return data['tournaments'][0]['name']


def get_cup_division(div: Dict) -> Optional[str]:
    """ 
        Extract the cup division name. 
    """
    try:
        return div['name']
    except KeyError:
        return None


def get_league_df(div: Dict) -> pd.DataFrame:
    """ 
        Normalize the JSON participants data into a pandas DataFrame. 
    """
    return pd.json_normalize(div['participants'])


def get_file_list(directory: str) -> List[str]:
    """ 
        Return a list of JSON files in the specified directory. 
    """
    return glob.glob(f'./bbc-json/{directory}/*.json')


def extract_date_from_filename(filename: str) -> str:
    """ 
        Extract the date from a JSON filename. 
    """
    return filename.split('/')[-1].split('.')[0]


def extract_player_id(long_id: str) -> str:
    """ 
        Extract the player ID from a long URN. 
    """
    return long_id.split(':')[-1]


def process_league_table_df(df: pd.DataFrame, league_name: str, cup_division: str) -> pd.DataFrame:
    """ 
        Add league name, reduce columns, and rename columns for the DataFrame. 
    """
    df['league_name'] = league_name

    df['cup_div'] = cup_division

    df = df[['league_name', 'cup_div', 'rank', 'name', 'matchesPlayed', 'wins', 'draws', 'losses', 'goalsScoredFor', 'goalsScoredAgainst', 'goalDifference', 'points']]

    return df.rename(columns={
        'name': 'team_name',
        'matchesPlayed' : 'p',
        'wins': 'w',
        'draws': 'd',
        'losses': 'l',
        'goalsScoredFor': 'gf',
        'goalsScoredAgainst': 'ga',
        'goalDifference': 'gd'
    })


def process_league_table(game_date: str, team_name: str='Tranmere Rovers') -> pd.DataFrame:
    """ 
        Process league data for a specific team and date. 
    """
    file = name_json_file(game_date)
    data = read_json_file(file)
    divs = get_divs(data)
    div = find_team_div(divs, team_name)

    if div:
        league_name = get_league_name(data)
        cup_div = get_cup_division(div)
        df = get_league_df(div)
        return process_league_table_df(df, league_name, cup_div)
    else:
        return pd.DataFrame()


def process_match_stats(data: Dict, game_date: str) -> pd.DataFrame:
    """
        Process match stats for an individual game.
    """
    both_teams = []

    try:
        for team in ['homeTeam', 'awayTeam']:
            team_name = data[team]['name']['fullName']
            team_venue = data[team]['alignment']

            team_stats = pd.json_normalize(data[team]['stats'])
            team_stats.columns = team_stats.columns.str.replace('.total', '')

            default_cols = team_stats.columns.tolist()

            team_stats['game_date'] = game_date
            team_stats['team_name'] = team_name
            team_stats['team_venue'] = team_venue

            df_cols = ['game_date', 'team_name', 'team_venue'] + default_cols
            team_stats = team_stats[df_cols]

            both_teams.append(team_stats)

        both_teams_df = pd.concat(both_teams)

        return both_teams_df

    except Exception as e:
        print(f"Error processing {game_date}: {e}")
        return pd.DataFrame()


def process_lineups_df(data: Dict) -> pd.DataFrame:
    """
        Process the lineups JSON data into a DataFrame.
    """
    both_teams = []

    for team in ['homeTeam', 'awayTeam']:
        try:
            team_name = data[team]['name']['fullName']
            team_venue = data[team]['alignment']
            formation = data[team]['formation']['value'].replace(' ', '')
            team_manager = data[team]['manager']['name']['full']
            players = data[team]['players']
            roles = ['starters', 'substitutes']

            for role in roles:
                player_list = players[role]

                for player in player_list:
                    surname = player['name']['last']
                    forename = player['name']['first']
                    short_name = player['name']['short']
                    full_name = f"{forename} {surname}"
                    player_id = extract_player_id(player['playerUrn'])
                    shirt_no = player['shirtNumber']
                    position = player['position']

                    if 'formationPlace' in player.keys():
                        formation_place = player['formationPlace']
                    else:
                        formation_place = None

                    is_captain = player['isCaptain']
                    cards = player['cards']

                    yellow_card = 0
                    min_yc = None
                    
                    red_card = 0
                    min_rc = None

                    if len(cards) > 0:
                        for card in cards:
                            if card['type'] == 'Yellow Card':
                                yellow_card = 1
                                min_yc = card['timeLabel']['value'].replace("'", '')
                            elif card['type'] == 'Red Card':
                                red_card = 1
                                min_rc = card['timeLabel']['value'].replace("'", '')

                    sub_off_period = None
                    sub_off_min = None
                    sub_off_reason = None
                    sub_replacement_id = None
                    sub_replacement_name = None

                    if 'substitutedOff' in player.keys():
                        sub_off = player['substitutedOff']
                        sub_off_period = sub_off['periodId']
                        sub_off_min = sub_off['timeMin']
                        sub_off_reason = sub_off['reason']
                        sub_replacement_id = sub_off['playerOnUrn']
                        sub_replacement_id = extract_player_id(sub_replacement_id)
                        sub_replacement_name = sub_off['playerOnName']

                    sub_on_period = None
                    sub_on_min = None
                    sub_on_reason = None
                    sub_replaced_id = None
                    sub_replaced_name = None

                    if 'substitutedOn' in player.keys():
                        sub_on = player['substitutedOn']
                        sub_on_period = sub_on['periodId']
                        sub_off_min = sub_on['timeMin']
                        sub_off_reason = sub_on['reason']
                        sub_replaced_id = sub_on['playerOffUrn']
                        sub_replaced_id = extract_player_id(sub_replaced_id)
                        sub_replaced_name = sub_on['playerOffName']

                    player_data = {
                        'game_date': game_date,
                        'team_name': team_name,
                        'team_venue': team_venue,
                        'formation': formation,
                        'team_manager': team_manager,
                        'surname': surname,
                        'forename': forename,
                        'player_name': full_name,
                        'short_name': short_name,
                        'player_id': player_id,
                        'shirt_no': shirt_no,
                        'position': position,
                        'formation_place': formation_place,
                        'is_captain': is_captain,
                        'yellow_card': yellow_card,
                        'min_yc': min_yc,
                        'red_card': red_card,
                        'min_rc': min_rc,
                        'sub_off_period': sub_off_period,
                        'sub_off_min': sub_off_min,
                        'sub_off_reason': sub_off_reason,
                        'sub_replacement_id': sub_replacement_id,
                        'sub_replacement_name': sub_replacement_name,
                        'sub_on_period': sub_on_period,
                        'sub_on_min': sub_on_min,
                        'sub_on_reason': sub_on_reason,
                        'sub_replaced_id': sub_replaced_id,
                        'sub_replaced_name': sub_replaced_name
                    }

                    both_teams.append(player_data)
            
            return pd.DataFrame(both_teams).sort_values(by=['game_date', 'team_venue', 'shirt_no'], ascending = [True, False, True]).reset_index(drop=True)
                    
        except Exception as e:
            print(f"Error processing {team} on {game_date}: {e}")
            return pd.DataFrame()


def process_officials(data: Dict, game_date: str) -> pd.DataFrame:
    all_officials = []

    if 'officials' in data.keys() and len(data['officials']) > 0:
        match_officials = []

        for official in data['officials']:
            
            if 'shortFirstName' in official.keys():
                forename = official['shortFirstName']
            else:
                forename = official['firstName']

            if 'shortLastName' in official.keys():
                surname = official['shortLastName']
            else:
                surname = official['lastName']

            official_name = f"{forename} {surname}"

            role = official['type']

            official_data = {
                'game_date': game_date,
                'surname': surname,
                'forename': forename,
                'name': official_name,
                'role': role
            }

            match_officials.append(official_data)
        return pd.DataFrame(all_officials)
    else:
        return pd.DataFrame()
    

def process_assists(data: Dict, game_date: str) -> pd.DataFrame:
    """
        Process assists data for an individual game.
        Takes in JSON data from a match_info file or API response.
    """
    all_assists = []

    if 'groupedActions' in data['sportDataEvent'].keys():
        grouped_actions = data['sportDataEvent']['groupedActions']

        for action in grouped_actions:
            group_name = action['groupName']['fullName']

            if group_name == 'Assists':

                team_actions = {
                    'homeTeamActions': data['sportDataEvent']['home']['fullName'],
                    'awayTeamActions': data['sportDataEvent']['away']['fullName']
                }

                for team in team_actions.key():
                    if team in action.keys():
                        team_assists = action[team]

                        for assist in team_assists:
                            assist_info = assist.split(' (')
                            assist_player = assist_info[0]
                            assist_min = assist_info[1].replace(')', '').replace("'", '').split(',')
                            for min in assist_min:
                                assist_min = min.strip()
                                assist_min_inj = None
                                if '+' in min:
                                    inj_assist = min.split('+')
                                    assist_min = inj_assist[0]
                                    assist_min_inj = inj_assist[1]
                                assist_data = {
                                    'game_date': game_date,
                                    'team_name': team_actions[team],
                                    'assist_player': assist_player,
                                    'assist_min': assist_min,
                                    'assist_min_inj': assist_min_inj
                                }
                                all_assists.append(assist_data)
        return pd.DataFrame(all_assists)
    else:
        print(f"No assists found for {game_date}")
        return pd.DataFrame()
    
def process_scores(data: Dict, game_date: str) -> pd.DataFrame:
    """
        Process HT and FT scores for an individual game.
        Takes in JSON data from a match_info file or API response.
    """

    all_score_data = []    
    for team in ['home', 'away']:
        team_info = data['sportDataEvent'][team]

        team_venue = team

        team_name = team_info['fullName']

        ht_score = team_info['runningScores']['halftime']

        ft_score = team_info['runningScores']['fulltime']

        if 'penaltyShootoutScore' in team_info['runningScores'].keys():
            pens_score = team_info['runningScores']['penaltyShootoutScore']
        else:
            pens_score = None

        scores_data = {
            'game_date': game_date,
            'team_name': team_name,
            'team_venue': team_venue,
            'ht_score': ht_score,
            'ft_score': ft_score,
            'pens_score': pens_score
        }

        all_score_data.append(scores_data)
    
    return pd.DataFrame(all_score_data)

def process_goals(data: Dict, game_date: str) -> pd.DataFrame:
    """
        Process goal data for an individual game.
        Takes in JSON data from a match_info file or API response.
    """
    all_goals = []
    
    for team in ['home', 'away']:
        team_info = data['sportDataEvent'][team]

        if 'actions' in team_info.keys():
            actions = team_info['actions']

            team_name = team_info['fullName']

            for action in actions:

                if action['actionType'] == 'goal':
                    
                    player_name = action['playerName']

                    bbc_player_id = action['playerUrn']
                    player_id = extract_player_id(bbc_player_id)

                    goals = action['actions']

                    for goal in goals:

                        goal_min = goal['timeLabel']['value'].replace("'", '')

                        goal_type = goal['type']
                        
                        goal_min_inj = None
                        if '+' in goal_min:
                            inj_goal = goal_min.split('+')
                            goal_min = inj_goal[0]
                            goal_min_inj = inj_goal[1]

                        goal_data = {
                            'game_date': game_date,
                            'team_name': team_name,
                            'player_name': player_name,
                            'bbc_player_id': player_id,
                            'goal_min': goal_min,
                            'goal_min_inj': goal_min_inj,
                            'goal_type': goal_type
                        }
                        all_goals.append(goal_data)

def process_sameday_fixtures(data: Dict, game_date: str) -> pd.DataFrame:
    all_sameday_fixtures = []

    games = data['events']

    for game in games:

            if game['status'] != 'Cancelled':

                    home_team = game['home']['fullName']

                    away_team = game['away']['fullName']
            
                    for team in ['home', 'away']:
                            
                            team_info = game[team]

                            team_name = team_info['fullName']

                            ht_score = team_info['runningScores']['halftime']

                            ft_score = team_info['runningScores']['fulltime']

                            if 'penaltyShootoutScore' in team_info['runningScores'].keys():
                                    pen_score = team_info['runningScores']['penaltyShootoutScore']
                            else:
                                    pen_score = None

                            scores_data = {
                                    'game_date': game_date,
                                    'home_team': home_team,
                                    'away_team': away_team,
                                    'team_name': team_name,
                                    'ht_score': ht_score,
                                    'ft_score': ft_score,
                                    'pen_score': pen_score
                            }

                            all_sameday_fixtures.append(scores_data)

    return pd.DataFrame(all_sameday_fixtures)


def process_sameday_fixture_scores(data: Dict, game_date: str) -> pd.DataFrame:
    games = data['events']
    
    all_sameday_scorers = []
    
    for game in games:

        if game['status'] != 'Cancelled':

                for team in ['home', 'away']:

                    team_info = game[team]

                    team_name = team_info['fullName']
    
                    if 'actions' in team_info.keys():
                        actions = team_info['actions']

                        for action in actions:

                                if action['actionType'] == 'goal':
                                
                                        player_name = action['playerName']

                                        bbc_player_id = action['playerUrn']
                                        player_id = extract_player_id(bbc_player_id)

                                        goals = action['actions']

                                        for goal in goals:

                                                goal_min = goal['timeLabel']['value'].replace("'", '')

                                                goal_min_inj = None
                                                if '+' in goal_min:
                                                        inj_goal = goal_min.split('+')
                                                        goal_min = inj_goal[0]
                                                        goal_min_inj = inj_goal[1]

                                                goal_type = goal['type']

                                                goal_data = {
                                                        'game_date': game_date,
                                                        'team_name': team_name,
                                                        'player_name': player_name,
                                                        'bbc_player_id': player_id,
                                                        'goal_min': goal_min,
                                                        'goal_min_inj': goal_min_inj,
                                                        'goal_type': goal_type
                                                }
                                                all_sameday_scorers.append(goal_data)
    return pd.DataFrame(all_sameday_scorers)


def process_commentary(data: Dict, game_date: str) -> pd.DataFrame:
    all_commentary = []

    pages = data

    for page in pages:
        comms = page['results']

        for comm in comms:

            comm_min = comm['dates']['time'].replace("'", '')

            if '+' in comm_min:
                inj_comm = comm_min.split('+')
                comm_min = inj_comm[0]
                comm_min_inj = inj_comm[1]
            else:
                comm_min_inj = None
            
            comm_text = comm['content']['model']['blocks'][0]['model']['blocks'][0]['model']['text']

            headline = comm['headline']

            if headline:
                headline = headline['model']['blocks'][0]['model']['text']

            comm_data = {
                'game_date': game_date,
                'comm_min': comm_min,
                'comm_min_inj': comm_min_inj,
                'comm_text': comm_text,
                'headline': headline
            }

            all_commentary.append(comm_data)

    return pd.DataFrame(all_commentary).sort_values(by=['game_date', 'comm_min']).reset_index(drop=True)