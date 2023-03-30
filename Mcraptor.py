'''
Module contains McRAPTOR implementation.
'''

import copy

from collections import deque as deque
from Mcraptor_functions import initialize_Mcraptor
from Mcraptor_functions import merge
from Mcraptor_functions import check_non_dominance
from Mcraptor_functions import give_non_dominating_labels
from Mcraptor_functions import get_latest_trip_new
from Mcraptor_functions import IVTT


def McRAPTOR(SOURCE: int, DESTINATION: int, DEPARTURE_TIME_IN_SEC: int, trips_in_route_dict: dict, stops_in_trip_dict: dict, routes_by_stop_dict: dict, stops_dict: dict, stops_file, footpath_dict: dict, NUMBER_OF_CRITERIA: int,idx_by_route_stop_dict: dict ,MAX_TRANSFER: int) -> tuple:
    '''

    McRAPTOR implementation.

    Args:
        SOURCE (int): stop id of source stop.
        DESTINATION (int): stop id of destination stop.
        DEPARTURE_TIME_IN_SEC (int): departure time in seconds.
        trips_in_route_dict (dict): preprocessed dict. Format {route_id: [trip_id]}.
        stops_in_trip_dict (dict): preprocessed dict. Format {trip_id: {stop_id: arrival_time at that stop}}.
        routes_by_stop_dict (dict): preprocessed dict. Format {route_id: [ids of stops in the route]}.
        stops_dict (dict): preprocessed dict. Format {route_id: [ids of stops in the route]}.
        stops_file (pandas.dataframe): having columns = ['stop_lat', 'stop_lon', 'stop_id'].
        footpath_dict (dict): preprocessed dict. Format {from_stop_id: [(to_stop_id, footpath_time)]}.
        NUMBER_OF_CRITERIA (int): number of criteria taken other than rounds.
        idx_by_route_stop_dict (dict): preprocessed dict. Format {(route id, stop id): stop index in route}.
        MAX_TRANSFER (int): maximum transfer limit.

    Returns:
            label_dict (dict): Nested dictionary that stores labels in the form of nested list for each stop at each round. Format-> {round: {stop_id: [[arrival_time, number_of_stops, IVTT, trip_id]] }}.
            inf_time (int): infinite time (datetime.datetime).
    '''

    # Initialization
    label_dict, star_label, marked_stop_dict, inf_time, marked_stop = initialize_Mcraptor(stops_file, SOURCE, MAX_TRANSFER)

    label_dict[0][SOURCE][0][0], label_dict[0][SOURCE][0][1], label_dict[0][SOURCE][0][2] = DEPARTURE_TIME_IN_SEC, 1, 0
    star_label[SOURCE][0][0], star_label[SOURCE][0][1], star_label[SOURCE][0][2] = DEPARTURE_TIME_IN_SEC, 1, 0

    # Main Code
    # Main code part 1
    for i in range(1, MAX_TRANSFER+1):
        print("Round", i)
        Q = {}
        while marked_stop:
            mark_stop = marked_stop.pop()
            for route in routes_by_stop_dict[mark_stop]:
                if route in Q.keys():
                    if stops_dict[route].index(mark_stop) < stops_dict[route].index(Q[route]):
                        Q[route] = mark_stop
                else:
                    Q[route] = mark_stop
            marked_stop_dict[mark_stop] = 0

        # Main code part 2
        for route in Q.keys():
            Br = []
            current_route_stops = stops_dict[route][idx_by_route_stop_dict[(route, Q[route])]:]
            for id,stop_in_route in enumerate(current_route_stops):
                ''' First step '''
                for label in Br:
                    t = label[-1]
                    label[0] = stops_in_trip_dict[t][stop_in_route]
                    label[1] = label[1] + 1
                    label[2] = IVTT(current_route_stops[id-1], stop_in_route, stops_in_trip_dict, label)

                ''' Second step '''
                Bkp = label_dict[i][stop_in_route]
                Br_new = []
                for Li in Br:
                    if check_non_dominance(Li, star_label[stop_in_route], NUMBER_OF_CRITERIA) and check_non_dominance(Li, star_label[DESTINATION], NUMBER_OF_CRITERIA):
                        Br_new.append(Li)
                        star_label[stop_in_route] = give_non_dominating_labels([Li] + star_label[stop_in_route], NUMBER_OF_CRITERIA)

                Bkp_new, newly_added_labels = merge(Bkp, Br_new, NUMBER_OF_CRITERIA)

                label_dict[i][stop_in_route] = Bkp_new
                if len(newly_added_labels) > 0:
                    marked_stop.append(stop_in_route)
                    marked_stop_dict[stop_in_route] = 1

                ''' Third step '''
                Bk_1p = label_dict[i-1][stop_in_route]
                temp_br, new_labels = merge(Br, Bk_1p, NUMBER_OF_CRITERIA)
                Br_updated = []
                for lab in temp_br:
                    if lab in new_labels:
                        Time = lab[0]
                        t = get_latest_trip_new(route, stop_in_route, Time, trips_in_route_dict, stops_in_trip_dict)
                        lab[-1] = t
                        if t != -1:
                            Br_updated.append(lab)
                    else:
                        Br_updated.append(lab)
                Br = [x for x in Br_updated]

        # Main code part 3
        marked_stop_copy = [*marked_stop]
        for mark_stop in marked_stop_copy:
            if mark_stop in footpath_dict.keys():
                for tup in footpath_dict[mark_stop]:
                    temp_bag = copy.deepcopy(label_dict[i][mark_stop])
                    for li in temp_bag:
                        li[0] = li[0] + tup[1]
                        li[1] += 1
                    Bkpj = label_dict[i][tup[0]]
                    Bkpj_new, new_lab = merge(Bkpj, temp_bag, NUMBER_OF_CRITERIA)
                    label_dict[i][tup[0]] = Bkpj_new
                    star_label[tup[0]] = give_non_dominating_labels(temp_bag + star_label[tup[0]], NUMBER_OF_CRITERIA)
                    if len(new_lab) > 0:
                        marked_stop.append(tup[0])
                        marked_stop_dict[tup[0]] = 1

        # Main code End
        if marked_stop == deque([]):
            break

    return label_dict, inf_time
