import pandas as pd

def run():
    prev_run_data = load_prev_run_data()
    json_log_increment = get_json_log_increment(prev_run_data.last_json_log_line)
    users_recently_pulled_ids = extract_pull_userids(json_log_increment)
    user_list_stats_df = load_user_lists_stats()
    not_done_users = exract_not_done_users_ids(user_list_stats_df)
    user_ids_for_if_done = calculate_intersection(not_done_users, users_recently_pulled_ids)

def get_json_log_increment(prev_last_line_num):
    return get_file_lines_from_line_num("run/json.log", prev_last_line_num)


def get_file_lines_from_line_num(file_name, line_num):
    result = []
    with open('run/json.log') as f:
        for i in range(line_num):
            next(f)
        for line in f:
            result.append(line)
