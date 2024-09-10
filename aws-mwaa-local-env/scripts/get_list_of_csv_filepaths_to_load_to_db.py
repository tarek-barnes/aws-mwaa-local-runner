import os

# def get_list_of_csv_filepaths_to_load_to_db(directory: str) -> list:
#     # return [f'{directory}/{k}' for k in os.listdir(directory) if os.path.isfile(k) if k.endswith('.csv')]
#     return [f'{directory}/{k}' for k in os.listdir(directory) if k.endswith('.csv')]

def get_list_of_csv_filepaths_to_load_to_db(directory: str) -> list:
    # return [f'{directory}/{k}' for k in os.listdir(directory) if os.path.isfile(k) if k.endswith('.csv')]
    return [f'{directory}/{k}' for k in os.listdir(directory) if k.endswith('.csv')]
