# -*- coding: utf-8 -*-
"""
Created on Sun Jul 15 15:15:52 2018

@author: leeyo
"""
import pandas as pd
import os
import queue
os.chdir(r'C:\Users\leeyo\OneDrive\DOCUME~3-DESKTOP-UT3R6M6-266\Columbia Univ\Heckaton')
event_code = pd.read_csv("NBA Hackathon - Event Codes.txt",delimiter="\t")
game_lineup = pd.read_csv('NBA Hackathon - Game Lineup Data Sample (50 Games).txt', delimiter = '\t')
play_by_play = pd.read_csv('NBA Hackathon - Play by Play Data Sample (50 Games).txt',delimiter = '\t')
def sorting(game):
    step_1 = game.sort_values(by = 'Period')
    step_2 = step_1.sort_values(by = 'PC_Time', ascending = False)
    step_3 = step_2.sort_values(by = 'WC_Time')
    sorted_game = step_3.sort_values(by = 'Event_Num')
    return sorted_game

def merging(sorted_game, game_lineup):
    single_gl = game_lineup[game_lineup.Game_id == sorted_game.Game_id.get_values()[0]]
    temp = single_gl.drop_duplicates(subset = 'Person_id')
    merge_temp1 = pd.merge(sorted_game, temp, how='left', left_on = 'Person1', right_on = 'Person_id')
    merged_df = pd.merge(merge_temp1, temp, how='left', left_on = 'Person2', right_on = 'Person_id')
    merged_df = merged_df.drop(['Game_id_y', 'Period_y', 'Person_id_x', 'status_x', 'Game_id', 'Period', 'Person_id_y', 'status_y'], axis = 1)
    return merged_df

def calculate_player_stat(merged_game, event_code):
    # Something is wrong with our data. I don't play basketball, so I looked up rules and there are only 3 ways
    # that a player can score: free throw, 3 points, and 2 points.
    # but sometimes, our data says hook shot was missed but option 1 indicates 2. Why?
    active_players = {} #creating a map
    score_updates = {}
    final_results = {}
    substitution = True
    leaving = queue.Queue()
    replacing = queue.Queue()
    #wait_queue = {'leaving': [], 'replacing': []}
    change_players = False
    for _, each_row in merged_game.iterrows():
        test1, test2 = match_events(each_row, event_code)
        if 'Start Period' in test1:
            temp = game_lineup.loc[(game_lineup['Game_id'] == each_row['Game_id_x']) & (game_lineup['Period'] == each_row['Period_x'])]    
            # Start of the period
            # temp contains 10 players information
            # map to list, faster since repeated insertion and deletion
            # Also needs to clear this at each starting point.
            for entity in active_players:
                active_players[entity] = []
            for _, player in temp.iterrows():
                try:
                    active_players[player['Team_id']].append(player['Person_id'])
                # key is each Team_id and it will be mapped to five players.
                except KeyError:
                    active_players[player['Team_id']] = [player['Person_id']]
        elif 'Made Shot' in test1:
            update_statistics(each_row, active_players, score_updates, final_results)
        elif 'Free Throw' in test1 and 'Technical' not in test2:
            # I guess this is where players can have multiple throws, and things get complicated
            current_shot, total_shot = matched['Action_Type_Description'].item().split('Free Throw ')[1].split(' of ')
            # as long as free throw attemps are remaining, players when foul was commited receives points
            current_shot = int(current_shot)
            total_shot = int(total_shot)
            update_statistics(each_row, active_players, score_updates, final_results)
            if current_shot == total_shot:
                if change_players:
                    replace_players(each_row, active_players, wait_queue, True)
                substitution = True
            elif current_shot < total_shot:
                substitution = False
                # can only change players after free throw is made
        elif 'Substitution' in test1:
            if not substitution:
                wait_queue['leaving'].append(each_row['Person1'])
                wait_queue['replacing'].append(each_row['Person2'])
                change_players = True
            else:
                leaving = each_row['Person1']
                replacing = each_row['Person2']
                for team in active_players:
                    previous_list = active_players[team]
                    if leaving in previous_list:
                        active_players[team].remove(leaving)
                        active_players[team].append(replacing)   
                wait_queue['leaving'] = []
                wait_queue['replacing'] = []
    return final_results
                    
                
def replace_players(each_row, active_players, wait_queue, change_players):
    if change_players:
        leaving = wait_queue['leaving']
        replacing = wait_queue['replacing']
    else:
        leaving = each_row['Person1']
        replacing = each_row['Person2']
    n = len(leaving)
    for team in active_players:
        previous_list = active_players[team]
        for i in range(n):
            if leaving[i] in previous_list:
                active_players[team].remove(leaving[i])
                active_players[team].append(replacing[i]) 
    wait_queue['leaving'] = []
    wait_queue['replacing'] = []
       
def update_statistics(each_row, active_players, score_updates, final_results):
    scored_team = each_row['Team_id_y']
    points = each_row['Option1']
    score_updates = {}
    for team in active_players:
        if(team == scored_team):
            # This team scored
            for member in active_players[team]:
                score_updates[member] = points
        else:
            for member in active_players[team]:
                score_updates[member] = -points
    for player, score in score_updates.items():
        try:
            final_results[player]
        except KeyError:
            final_results[player] = 0
        final_results[player] += score
                 
             
             
            
def match_events(single_row, event_code):
    event_type = event_code.loc[(event_code['Event_Msg_Type'] == single_row['Event_Msg_Type']) & (event_code['Action_Type'] == single_row['Action_Type'])]
    test_event_type = event_type['Event_Msg_Type_Description'].item()
    test_action_type = event_type['Action_Type_Description'].item()
    return test_event_type, test_action_type

game_id_set = game_lineup['Game_id'].unique()
results = {}
i = 0
for each_game in game_id_set:
    game = play_by_play[play_by_play.Game_id == each_game] # contains dataframe that only corresponds to one game
    sorted_game = sorting(game) # sorting as told from instruction
    merged_game = merging(sorted_game, game_lineup)
    results[each_game] = calculate_player_stat(merged_game, event_code)
    i = i + 1
