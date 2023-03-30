'''
Module contains functions to load the GTFS data.
'''



import pickle



def load_all_dict(FOLDER: str):
    """
    Args:
        FOLDER (str): network folder.

    Returns:
        stops_dict (dict): preprocessed dict. Format {route_id: [ids of stops in the route]}.
        trips_in_route_dict (dict): preprocessed dict. Format keys: trip ID, values: list of trips in the increasing order of start time. Format-> dict[route_ID] = [trip_1, trip_2].
        stops_in_trip_dict (dict): preprocessed nested dictionary with primary key: trip_id and secondary key: stop_id with value: arrival time of that trip on that stop. Format-> {trip_id: {stop_id: arrival_time}}
        footpath_dict (dict): preprocessed dict. Format {from_stop_id: [(to_stop_id, footpath_time)]}.
        routes_by_stop_dict (dict): preprocessed dict. Format {stop_id: [id of routes passing through stop]}.
        idx_by_route_stop_dict (dict): preprocessed dict. Format {(route id, stop id): stop index in route}.
    """
    with open(f'./dict_builder/{FOLDER}/routes_by_stop.pkl', 'rb') as file:
        routes_by_stop_dict = pickle.load(file)
    with open(f'./dict_builder/{FOLDER}/stops_dict_pkl.pkl', 'rb') as file:
        stops_dict = pickle.load(file)
    with open(f'./dict_builder/{FOLDER}/trips_in_route_dict_pkl.pkl', 'rb') as file:
        trips_in_route_dict = pickle.load(file)
    with open(f'./dict_builder/{FOLDER}/stops_in_trip_dict_pkl.pkl', 'rb') as file:
        stops_in_trip_dict = pickle.load(file)
    with open(f'./dict_builder/{FOLDER}/transfers_dict_pkl.pkl', 'rb') as file:
        footpath_dict = pickle.load(file)
    with open(f'./dict_builder/{FOLDER}/idx_by_route_stop.pkl', 'rb') as file:
        idx_by_route_stop_dict = pickle.load(file)

    return routes_by_stop_dict, stops_dict, trips_in_route_dict, stops_in_trip_dict, footpath_dict, idx_by_route_stop_dict



def load_all_db(FOLDER: str):
    """
    Args:
        FOLDER (str): path to network folder.

    Returns:
        stops_file (pandas.dataframe): dataframe with stop details.
        trips_file (pandas.dataframe): dataframe with trip details.
        stop_times_file (pandas.dataframe): dataframe with stoptimes details.
        transfers_file (pandas.dataframe): dataframe with transfers (footpath) details.
    """
    import pandas as pd
    from Miscellenous_functions import convert_to_sec

    path = f"./GTFS/{FOLDER}"
    stops_file = pd.read_csv(f'{path}/stops.txt', sep=',')
    trips_file = pd.read_csv(f'{path}/trips.txt', sep=',')
    stop_times_file = pd.read_csv(f'{path}/stop_times.txt', sep=',')
    if type(stop_times_file.arrival_time.iloc[0]) == str:
        stop_times_file["arrival_time_in_sec"] = stop_times_file["arrival_time"].apply(convert_to_sec)
    if "route_id" not in stop_times_file.columns:
        stop_times_file = pd.merge(stop_times_file, trips_file, on='trip_id')
    transfers_file = pd.read_csv(f'{path}/transfers.txt', sep=',')

    return stops_file, trips_file, stop_times_file, transfers_file