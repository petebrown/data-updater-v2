import datetime
import pandas as pd

# Current R datasets
results = pd.read_csv('./data-r/results.csv')
player_apps_1 = pd.read_csv('./data-r/player_apps.csv')
goals_df_1 = pd.read_csv('./data-r/goals.csv')
subs_1 = pd.read_csv('./data-r/subs.csv')
sub_mins_1 = pd.read_csv('./data-r/sub_mins.csv')
yellow_cards_1 = pd.read_csv('./data-r/yellow_cards.csv')
red_cards_1 = pd.read_csv('./data-r/red_cards.csv')

def get_r_df(file_name):
    return pd.read_csv(f'./data-r/{file_name}.csv')

def data_url(extension, local=True):
    if local:
        return f'./data/{extension}'
    else:
        return f'https://raw.githubusercontent.com/petebrown/data-updater/refs/heads/main/data/{extension}'
    
def today():
    return datetime.datetime.now().strftime('%Y-%m-%d')

def check_date(df, date):
    if not df.query('game_date==@date').empty:
        return True
    else:
        return False

def remove_date_record(df, date):
    return df.query('game_date!=@date')

def get_results(dates=[today()]):
    results = get_r_df('results')
    # scores = pd.read_csv(data_url('scores.csv'))

    for date in dates:
        if check_date(results, date):

            season = '2024/25'
            game_date = date
            opposition = scores.query('game_date==@date & team_name!="Tranmere Rovers"')['team_name'].values[0]
            if scores.query('game_date==@date & team_name=="Tranmere Rovers"')['team_venue'].values[0] == 'home':
                venue = 'H'
            else:
                venue = 'A'

            goals_for = scores.query('game_date==@date & team_name=="Tranmere Rovers"')['ft_score'].values[0]
            goals_against = scores.query('game_date==@date & team_name!="Tranmere Rovers"')['ft_score'].values[0]

            goal_diff = goals_for - goals_against

            if goals_for > goals_against:
                outcome = 'W'
            elif goals_for < goals_against:
                outcome = 'L'
            else:
                outcome = 'D'

            score = str(goals_for) + '-' + str(goals_against)

            competition = match_info.query('game_date==@date')['competition'].values[0]

            if competition == 'League Two':
                game_type = 'League'
                league_tier = 4
            else:
                game_type = 'Cup'
                league_tier = ''


            if competition == 'League Two':
                league_pos = league_tables.query('game_date==@date & team_name=="Tranmere Rovers"')['rank'].values[0]
                pts = league_tables.query('game_date==@date & team_name=="Tranmere Rovers"')['points'].values[0]
                generic_comp = 'Football League'
            else:
                league_pos = ''
                pts = ''


            if 'League Cup' in competition:
                generic_comp = 'League Cup'
            elif 'FA Cup' in competition:
                generic_comp = 'FA Cup'
            elif 'EFL Trophy' in competition:
                generic_comp = "Associate Members' Cup"

            attendance = match_info.query('game_date==@date')['attendance'].values[0]

            manager = lineups.query('game_date==@date & team_name=="Tranmere Rovers"')['team_manager'].values[0]

            ko_time = match_info.query('game_date==@date')['ko_time'].values[0]
            cup_round=''
            cup_leg=''
            cup_stage=''
            cup_replay=''
            cup_section=''
            aet=''
            pen_outcome=''
            pen_score=''
            pen_gf=''
            pen_ga=''
            agg_outcome=''
            agg_score=''
            agg_gf=''
            agg_ga=''
            away_goal_outcome=''
            gg_outcome = ''
            decider = ''
            cup_outcome = ''
            outcome_desc = ''
            game_length = 90
            stadium = match_info.query('game_date==@date')['venue'].values[0]
            referee = officials.query('game_date==@date & role=="Referee"')['name'].values[0]

            match_record = pd.DataFrame({
                'season': season,
                'game_date': game_date,
                'game_no': '',
                'opposition': opposition,
                'venue': venue,
                'score': score,
                'outcome': outcome,
                'goals_for': goals_for,
                'goals_against': goals_against,
                'goal_diff': goal_diff,
                'game_type': game_type,
                'competition': competition,
                'generic_comp': generic_comp,
                'ssn_comp_game_no': '',
                'league_tier': league_tier,
                'league_pos': league_pos,
                'pts': pts,
                'attendance': attendance,
                'weekday': '',
                'manager': manager,
                'ko_time': ko_time,
                'cup_round': cup_round,
                'cup_leg': cup_leg,
                'cup_stage': cup_stage,
                'cup_replay': cup_replay,
                'cup_section': cup_section,
                'aet': aet,
                'pen_outcome': pen_outcome,
                'pen_score': pen_score,
                'pen_gf': pen_gf,
                'pen_ga': pen_ga,
                'agg_outcome': agg_outcome,
                'agg_score': agg_score,
                'agg_gf': agg_gf,
                'agg_ga': agg_ga,
                'away_goal_outcome': away_goal_outcome,
                'gg_outcome': gg_outcome,
                'decider': decider,
                'cup_outcome': cup_outcome,
                'outcome_desc': outcome_desc,
                'game_length': game_length,
                'stadium': stadium,
                'referee': referee
            }, index=[0])

            results = pd.concat([results, match_record])

            results['game_no'] = results.groupby('season').cumcount() + 1
            
            results['ssn_comp_game_no'] = match_record.groupby(['season', 'competition']).cumcount() + 1

            results['weekday'] = pd.to_datetime(match_record['game_date']).dt.day_name()

            results = results.drop_duplicates().reset_index(drop=True)
            return results
        else:
            print(f'No match data for {date}')
    
def get_player_apps(dates=[today()]):
    # lineups = pd.read_csv(data_url('lineups.csv'))
    # squad_nos = pd.read_csv('./data/squad_nos.csv')

    player_apps_df = pd.DataFrame()

    for date in dates:

        starters = lineups.query('game_date==@date & team_name=="Tranmere Rovers" & position != "Substitute"')[['game_date', 'shirt_no']]

        starters['role'] = 'starter'

        starters = starters.merge(squad_nos.drop(columns=['season']), left_on='shirt_no', right_on='squad_no', how='left')

        subs = lineups.query('game_date==@date & team_name=="Tranmere Rovers" & position == "Substitute" & sub_off_min.notnull()')[['game_date', 'shirt_no']]

        subs['role'] = 'sub'

        subs = subs.merge(squad_nos.drop(columns=['season']), left_on='shirt_no', right_on='squad_no', how='left')

        player_apps = pd.concat([starters, subs]).sort_values(['role', 'shirt_no']).drop(columns=['squad_no'])[['game_date', 'player_name', 'shirt_no', 'role']].reset_index(drop=True)

        player_apps_df = pd.concat([player_apps_df, player_apps]).drop_duplicates().reset_index(drop=True)


    return player_apps_df

def get_yellow_cards(dates=[today()]):
    # squad_nos = pd.read_csv('./data/squad_nos.csv')

    df = pd.DataFrame()

    for date in dates:
        yellow_cards = lineups.query('game_date==@date & team_name=="Tranmere Rovers" & yellow_card != 0')[['game_date', 'shirt_no', 'yellow_card', 'min_yc']]

        yellow_cards = yellow_cards.merge(squad_nos.drop(columns=['season']), left_on='shirt_no', right_on='squad_no', how='left')[['game_date', 'player_name', 'min_yc']]

        df = pd.concat([df, yellow_cards]).drop_duplicates().reset_index(drop=True)
    
    return df

def get_sub_mins(dates=[today()]):
    # squad_nos = pd.read_csv('./data/squad_nos.csv')
    sub_mins_df = pd.DataFrame()

    for date in dates:

        subs_on = lineups.query('game_date==@date & team_name=="Tranmere Rovers" & position == "Substitute" & sub_off_min.notnull()')[['game_date', 'shirt_no', 'sub_off_min']].rename(columns={'sub_off_min': 'min_on'})
        
        subs_off = lineups.query('game_date==@date & team_name=="Tranmere Rovers" & position != "Substitute" & sub_off_min.notnull()')[['game_date', 'shirt_no', 'sub_off_min']].rename(columns={'sub_off_min': 'min_off'})
        
        sub_mins = pd.concat([subs_on, subs_off]).merge(squad_nos.drop(columns=['season']), left_on='shirt_no', right_on='squad_no', how='left')[['game_date', 'player_name', 'min_off', 'min_on']]

        sub_mins_df = pd.concat([sub_mins_df, sub_mins]).drop_duplicates().reset_index(drop=True)

        sub_mins_df.loc[sub_mins_df.min_on > 90, 'min_on'] = 90
        sub_mins_df.loc[sub_mins_df.min_off > 90, 'min_off'] = 90

    return sub_mins_df

subs_df = pd.DataFrame()

def get_subs(dates=[today()]):
    # lineups = pd.read_csv(data_url('lineups.csv'))
    subs_df = pd.DataFrame()

    for date in dates: 
        subs_on = lineups.query('game_date==@date & team_name=="Tranmere Rovers" & position == "Substitute" & sub_off_min.notnull()')[['game_date', 'shirt_no', 'sub_replaced_name', 'sub_off_min']] \
            .merge(bbc_players, left_on='shirt_no', right_on='shirt_no', how='left') \
                .merge(bbc_players, left_on='sub_replaced_name', right_on='short_name', how='left') \
                    .rename(columns={'shirt_no_x': 'shirt_no', 'shirt_no_y': 'on_for'}) \
                        .merge(squad_nos.drop(columns=['season']), left_on='shirt_no', right_on='squad_no', how='left') \
                            [['game_date', 'shirt_no', 'player_name', 'on_for']]


        subs_off = lineups.query('game_date==@date & team_name=="Tranmere Rovers" & position != "Substitute" & sub_off_min.notnull()')[['game_date', 'shirt_no', 'sub_off_min', 'sub_replacement_name']] \
            .merge(bbc_players, left_on='shirt_no', right_on='shirt_no', how='left') \
                .merge(squad_nos, left_on='shirt_no', right_on='squad_no', how='left') \
                    .merge(bbc_players, left_on='sub_replacement_name', right_on='short_name', how='left') \
                        [['game_date', 'squad_no', 'player_name', 'shirt_no_y']] \
                            .rename(columns={'squad_no': 'shirt_no', 'shirt_no_y': 'off_for'}) 

        subs = pd.concat([subs_on, subs_off]) \
            [['game_date', 'shirt_no', 'player_name', 'on_for', 'off_for']] \
                .drop_duplicates().reset_index(drop=True)
        
        subs_df = pd.concat([subs_df, subs]).drop_duplicates().reset_index(drop=True)

    return subs_df

def get_goals(date=[today()]):
    df = pd.DataFrame()

    df = goals.query('team_name=="Tranmere Rovers"').copy()

    df.loc[df.goal_type == 'Penalty', 'penalty'] = 1
    df.loc[df.goal_type != 'Penalty', 'penalty'] = 0

    df.loc[df.goal_type == 'Own Goal', 'own_goal'] = 1
    df.loc[df.goal_type != 'Own Goal', 'own_goal'] = 0

    df = df.query('game_date > "2024-08-01"') \
        .merge(bbc_name_match, left_on='player_name', right_on='short_name', how='left') \
        .drop(columns=['short_name', 'player_name_x']) \
        .rename(columns={'player_name_y': 'player_name'}) \
        [['game_date', 'player_name', 'goal_min', 'penalty', 'own_goal']]

    df.loc[df.own_goal==1, 'player_name'] = 'OG'

    df = df.query('game_date==@date')

    return df

def merge_dataframes(df1, df2):
    if df2.empty:
        return df1
    else:
        return pd.concat([df1, df2]).reset_index(drop=True)

# Input data
goals = pd.read_csv('./data/goals.csv')
league_tables = pd.read_csv('./data/league_tables.csv')
lineups = pd.read_csv('./data/lineups.csv')
match_info = pd.read_csv('./data/match_info.csv')
officials = pd.read_csv('./data/officials.csv')
scores = pd.read_csv('./data/scores.csv')
squad_nos = pd.read_csv('./data/squad_nos.csv')

bbc_players = lineups.query("game_date > '2024-08-01' & team_name=='Tranmere Rovers'") \
        [['shirt_no', 'short_name']] \
            .drop_duplicates() \
                .sort_values('shirt_no') \
                    .reset_index(drop=True)

bbc_name_match = bbc_players.merge(squad_nos, left_on='shirt_no', right_on='squad_no', how='left').drop(columns=['squad_no'])[['short_name', 'player_name']]

# Updates
player_apps_df = get_player_apps()
goals_df = get_goals()
subs_df = get_subs()
sub_mins_df = get_sub_mins()
yellow_cards_df = get_yellow_cards()
red_cards_df = pd.DataFrame()

# Add updates to current R datasets
results = get_results() # Already combined

player_apps = merge_dataframes(player_apps_1, player_apps_df)

goals_df = merge_dataframes(goals_df_1, goals_df)

subs = merge_dataframes(subs_1, subs_df)

sub_mins = merge_dataframes(sub_mins_1, sub_mins_df)

yellow_cards = merge_dataframes(yellow_cards_1, yellow_cards_df)

try:
    red_cards = merge_dataframes(red_cards_1, red_cards_df)
except:
    red_cards = red_cards_1

def update_csvs():
    dfs = [
        ('results', results),
        ('player_apps', player_apps),
        ('goals', goals_df),
        ('subs', subs),
        ('sub_mins', sub_mins),
        ('yellow_cards', yellow_cards),
        ('red_cards', red_cards)
    ]

    for df in dfs:
        file_name = df[0]
        data = df[1]
        data.to_csv(f'./data-r/{file_name}.csv', index=False)