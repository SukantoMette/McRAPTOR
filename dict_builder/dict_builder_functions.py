"""
Module defines a function to save (using pickling) the GTFS information in the form of a dictionary.
This is done for easy/faster data lookup.
"""

import pickle
from tqdm import tqdm
import numpy as np

def build_save_route_by_stop(stop_times_file, FOLDER: str) -> dict:
    """
    This function saves a dictionary to provide easy access to all the routes passing through a stop_id.

    Args:
        stop_times_file (pandas.dataframe): stop_times.txt file in GTFS.
        FOLDER (str): path to network folder.

    Returns:
        route_by_stop_dict_new (dict): keys: stop_id, values: list of routes passing through the stop_id. Format-> dict[stop_id] = [route_id]
    """

    print("building routes_by_stop")
    routes_groupby_stops = stop_times_file.groupby("stop_id")["route_id"]
    route_by_stop_dict = {stop_id: list(np.unique(np.array(list(route)))) for stop_id, route in tqdm(routes_groupby_stops)}
    with open(f'./dict_builder/{FOLDER}/routes_by_stop.pkl', 'wb') as pickle_file:
        pickle.dump(route_by_stop_dict, pickle_file)
    print("routes_by_stop done")

    return route_by_stop_dict

def build_save_stops_dict(stop_times_file, FOLDER: str)-> dict:
    """
    This function saves a dictionary to provide easy access to all the stops in the route.

    Args:
        stop_times_file (pandas.dataframe): stop_times.txt file in GTFS.
        FOLDER (str): path to network folder.

    Returns:
        stops_dict (dict): keys: route_id, values: list of stop id in the route_id. Format-> dict[route_id] = [stop_id]
    """

    print("building stops dict..")
    stops_groupby_routes = stop_times_file.groupby("route_id")[["stop_sequence", "stop_id"]]
    stops_dict = {route_id: list(route.sort_values("stop_sequence").stop_id.unique()) for route_id, route in tqdm(stops_groupby_routes)}
    with open(f'./dict_builder/{FOLDER}/stops_dict_pkl.pkl', 'wb') as pickle_file:
        pickle.dump(stops_dict, pickle_file)
    print("stops_dict done")

    return stops_dict


def build_save_trips_in_route_dict(stop_times_file, FOLDER: str) -> dict:
    """
    This function saves a dictionary to provide easy access to all the trips passing along a route id. Trips are sorted
    in the increasing order of departure time.

    Args:
        stop_times_file (pandas.dataframe): stop_times.txt file in GTFS.
        FOLDER (str): path to network folder.

    Returns:
        trips_in_route_dict (dict): keys: route ID, values: list of trips in the increasing order of start time. Format-> dict[route_ID] = [trip_1, trip_2]
    """

    print("building stoptimes dict..")
    trips_groupby_routes = stop_times_file[stop_times_file.stop_sequence == 0].groupby("route_id")[["arrival_time_in_sec", "trip_id"]]
    trips_in_route_dict = {route_id: list(route.sort_values("arrival_time_in_sec").trip_id) for route_id, route in tqdm(trips_groupby_routes)}
    with open(f'./dict_builder/{FOLDER}/trips_in_route_dict_pkl.pkl', 'wb') as pickle_file:
        pickle.dump(trips_in_route_dict, pickle_file)
    print("stoptimes dict done")

    return trips_in_route_dict

def build_save_stops_in_trip_dict(stop_times_file, FOLDER: str) -> dict:
    """
        This function saves a dictionary to provide easy access to all the stop arrival time, stored in increasing order of arrival time.

        Args:
            stop_times_file (pandas.dataframe): stop_times.txt file in GTFS.
            FOLDER (str): path to network folder.

        Returns:
            stops_in_trip_dict (dict): nested dictionary with primary key: trip_id and secondary key: stop_id with value: arrival time of that trip on that stop. Format-> {trip_id: {stop_id: arrival_time}}
        """

    print("building stops_in_trip dict..")
    stops_groupby_trips = stop_times_file.groupby("trip_id")[["stop_id", "arrival_time_in_sec"]]
    stops_in_trip_dict = {}
    for trip_id, content in tqdm(stops_groupby_trips):
        content.sort_values("arrival_time_in_sec")
        temp = {}
        for i in range(content.shape[0]):
            temp[content.stop_id.iloc[i]] = float(content.arrival_time_in_sec.iloc[i])
        stops_in_trip_dict[trip_id] = temp

    with open(f'./dict_builder/{FOLDER}/stops_in_trip_dict_pkl.pkl', 'wb') as pickle_file:
        pickle.dump(stops_in_trip_dict, pickle_file)
    print("stops_in_trip dict done")

    return stops_in_trip_dict

def build_save_footpath_dict(transfers_file, FOLDER: str)-> dict:
    """
    This function saves a dictionary to provide easy access to all the footpaths through a stop id.

    Args:
        transfers_file (pandas.dataframe): dataframe with transfers (footpath) details.
        FOLDER (str): path to network folder.

    Returns:
        footpath_dict (dict): keys: from stop_id, values: list of tuples of form (to stop id, footpath duration). Format-> dict[stop_id]=[(stop_id, footpath_duration)]
    """

    print("building footpath dict..")
    footpath_stops_groupby_stops = transfers_file.groupby("from_stop_id")[["to_stop_id", "min_transfer_time"]]
    footpath_dict = {}
    for stop_id, content in tqdm(footpath_stops_groupby_stops):
        temp = []
        for i in range(content.shape[0]):
            temp1 = (content.to_stop_id.iloc[i], float(content.min_transfer_time.iloc[i]))
            temp.append(temp1)
        footpath_dict[stop_id] = temp

    with open(f'./dict_builder/{FOLDER}/transfers_dict_pkl.pkl', 'wb') as pickle_file:
        pickle.dump(footpath_dict, pickle_file)
    print("footpath_dict done")

    return footpath_dict

def stop_idx_in_route(stop_times_file, FOLDER: str)-> dict:
    """
    This function saves a dictionary to provide easy access to index of a stop in a route.

    Args:
        stop_times_file (pandas.dataframe): stop_times.txt file in GTFS.
        FOLDER (str): path to network folder.

    Returns:
        idx_by_route_stop_dict (dict): Keys: (route id, stop id), value: stop index. Format {(route id, stop id): stop index in that route}.
    """

    print("building idx_by_route_stop dict..")
    pandas_group = stop_times_file.groupby(["route_id","stop_id"])
    idx_by_route_stop = {route_stop_pair:details.stop_sequence.iloc[0] for route_stop_pair, details in pandas_group}

    with open(f'./dict_builder/{FOLDER}/idx_by_route_stop.pkl', 'wb') as pickle_file:
        pickle.dump(idx_by_route_stop, pickle_file)
    print("idx_by_route_stop done")

    return idx_by_route_stop
