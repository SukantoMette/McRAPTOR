'''
Module contains miscellaneous functions for reading test case, to print network details, to print shortest path query parameters,
to print the pareto optimal set of journeys, convert timestamp to total seconds lapsed with respect to base timestamp.
'''

import datetime as dt

def read_testcase(FOLDER: str) -> tuple:
    """
    Reads the GTFS network and preprocessed dict. If the dicts are not present, dict_builder_functions are called to construct them.

    Args:
        FOLDER (str): GTFS path

    Returns:
        stops_file (pandas.dataframe):  stops.txt file in GTFS.
        trips_file (pandas.dataframe): trips.txt file in GTFS.
        stop_times_file (pandas.dataframe): stop_times.txt file in GTFS.
        transfers_file (pandas.dataframe): dataframe with transfers (footpath) details.
        stops_dict (dict): keys: route_id, values: list of stop id in the route_id. Format-> dict[route_id] = [stop_id]
        trips_in_route_dict (dict): keys: route ID, values: list of trips in the increasing order of start time. Format-> dict[route_ID] = [trip_1, trip_2].
        stops_in_trip_dict (dict): nested dictionary with primary key: trip_id and secondary key: stop_id with value: arrival time of that trip on that stop. Format-> {trip_id: {stop_id: arrival_time}}
        footpath_dict (dict): keys: from stop_id, values: list of tuples of form (to stop id, footpath duration). Format-> dict[stop_id]=[(stop_id, footpath_duration)]
        route_by_stop_dict_new (dict): keys: stop_id, values: list of routes passing through the stop_id. Format-> dict[stop_id] = [route_id]
        idx_by_route_stop_dict (dict): preprocessed dict. Format {(route id, stop id): stop index in route}.

    """
    import gtfs_loader
    from dict_builder import dict_builder_functions
    stops_file, trips_file, stop_times_file, transfers_file = gtfs_loader.load_all_db(FOLDER)
    try:
        routes_by_stop_dict, stops_dict, trips_in_route_dict, stops_in_trip_dict, footpath_dict, idx_by_route_stop_dict = gtfs_loader.load_all_dict(FOLDER)
    except FileNotFoundError:
        stops_dict = dict_builder_functions.build_save_stops_dict(stop_times_file, FOLDER)
        trips_in_route_dict = dict_builder_functions.build_save_trips_in_route_dict(stop_times_file, FOLDER)
        stops_in_trip_dict = dict_builder_functions.build_save_stops_in_trip_dict(stop_times_file, FOLDER)
        routes_by_stop_dict = dict_builder_functions.build_save_route_by_stop(stop_times_file, FOLDER)
        footpath_dict = dict_builder_functions.build_save_footpath_dict(transfers_file, FOLDER)
        idx_by_route_stop_dict = dict_builder_functions.stop_idx_in_route(stop_times_file, FOLDER)

    return stops_file, trips_file, stop_times_file, transfers_file, stops_dict, trips_in_route_dict, stops_in_trip_dict, footpath_dict, routes_by_stop_dict, idx_by_route_stop_dict

def print_network_details(transfers_file, trips_file, stops_file) -> None:
    """
    Prints the network details like number of routes, trips, stops, footpath

    Args:
        transfers_file (pandas.dataframe): dataframe with transfers (footpath) details.
        trips_file (pandas.dataframe): trips.txt file in GTFS.
        stops_file (pandas.dataframe): stops.txt file in GTFS.

    Returns:
        None
    """
    print("___________________________Network Details__________________________")
    print("| No. of Routes |  No. of Trips | No. of Stops | No. of Footapths  |")
    print(
        f"|     {len(set(trips_file.route_id))}      |  {len(set(trips_file.trip_id))}       | {len(set(stops_file.stop_id))}        | {len(transfers_file)}             |")
    print("____________________________________________________________________")

    return None

def print_query_parameter(SOURCE, DESTINATION, DEPARTURE_TIME, NUMBER_OF_CRITERIA, MAX_TRANSFER):
    """
    Prints the input parameters related to the shortest path query.
    Args:
        SOURCE (int): source stop id.
        DESTINATION (int): destination stop id.
        DEPARTURE_TIME (int): departure time.
        NUMBER_OF_CRITERIA (int): number of criteria taken other than rounds.
        MAX_TRANSFER (int): Max transfer limit

    Returns:
        None

    """

    print()
    print()
    print("___________________Query Parameters__________________")
    print("Network: Switzerland")
    print(f"SOURCE stop id: {SOURCE}")
    print(f"DESTINATION stop id: {DESTINATION}")
    print(f"Departure Time: {DEPARTURE_TIME}")
    print(f"No. of Criterias: {NUMBER_OF_CRITERIA}")
    print(f"Maximum Transfer allowed: {MAX_TRANSFER}")
    print()
    print()

    return None


def print_output(final_label, DESTINATION, start_time, last_time, inf_time, MAX_TRANSFER):
    """
    prints the pareto optimal journeys in increasing order of number of round.

    Args:
        final_label (dict): Nested dictionary that stores labels in the form of nested list for each stop at each round. Format-> {round: {stop_id: [[arrival_time, number_of_stops, IVTT, trip_id]] }}
        DESTINATION (int): destination stop id.
        start_time (int): start time of the McRAPTOR algorithm, used to find the run time of the algorithm.
        last_time (int): end time of the McRAPTOR algorithm, used to find the run time of the algorithm.
        inf_time (int): infinite time (datetime.datetime).
        MAX_TRANSFER (int): Max transfer limit

    Returns:
        None

    """
    print("___________________Output__________________")

    for i in range(1, MAX_TRANSFER+1):
        for label in final_label[i][DESTINATION]:
            if label[0] != inf_time:
                print("time=",dt.datetime.strptime("1970-01-01 00:00:00", '%Y-%m-%d %H:%M:%S') + dt.timedelta(0, label[0]),
                      "No. of Stops=", label[1], "IVTT = ", (label[2]/60), "min ", "with number of trip=", i)
        # print()

    flag = 0
    for j in range(1, MAX_TRANSFER+1):
        for label in final_label[j][DESTINATION]:
            if label[0] != inf_time:
                flag = 1
                continue
        if flag:
            break
    else:
        print("NO JOURNEY IS AVAILABLE WITH MAXIMUM 5 TRANSFERS")

    print()
    print("time taken by algorithm in seconds =", last_time - start_time)

    return None


def convert_to_sec(string):
    """
    convert the given timestamp into total seconds lapsed from a base year timestamp("1970-01-01 00:00:00")

    Args:
        string (str): denotes the timestamp in the form of '%Y-%m-%d %H:%M:%S'.

    Returns:
        sec (int): time gap between the given timestamp and base timestamp("1970-01-01 00:00:00") in seconds.

    """
    timedelta = dt.datetime.strptime(string, '%Y-%m-%d %H:%M:%S') - dt.datetime.strptime("1970-01-01 00:00:00", '%Y-%m-%d %H:%M:%S')
    sec = timedelta.total_seconds()

    return sec