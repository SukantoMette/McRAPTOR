"""
Module contains function related to McRAPTOR.
"""

import copy
import numpy as np
import datetime as dt

from collections import deque as deque


def initialize_Mcraptor(stops_file, SOURCE: int, MAX_TRANSFER: int) -> tuple:
    '''
    Initialize values for McRAPTOR.

    Args:
        stops_file (pandas.dataframe): dataframe with stop details.
        SOURCE (int): stop id of source stop.
        MAX_TRANSFER (int): maximum transfer limit.

    Returns:
        marked_stop_dict (dict): Binary variable indicating if a stop is marked. Keys: stop Id, value: 0 or 1.
        label (dict): nested dict to maintain label. Format {round : {stop_id: arrival_time in seconds}}.
        star_label (dict): dict to maintain best labels for each stop. Format-> {stop_id: [[arrival_time, number_of_stops, IVTT, trip_id]] }
        inf_time (int): Variable indicating infinite time.
        marked_stop (deque): deque to store marked stop.

    '''

    timedelta = dt.datetime.strptime("2021-06-10 23:59:59", '%Y-%m-%d %H:%M:%S') - dt.datetime.strptime("1970-01-01 00:00:00", '%Y-%m-%d %H:%M:%S')
    inf_time = timedelta.total_seconds()
    inf_number_stops = 100000
    inf_IVTT = 10000000000
    initial_trip_id = -1

    label = {}
    for i in range(MAX_TRANSFER + 1):
        label[i] = {}
        for stop in stops_file.stop_id:
            label[i][stop] = [[inf_time, inf_number_stops, inf_IVTT, initial_trip_id]]

    star_label = {stop_id: [[inf_time, inf_number_stops, inf_IVTT, initial_trip_id]] for stop_id in stops_file.stop_id}

    marked_stop_dict = {stop_id: 0 for stop_id in stops_file.stop_id}
    marked_stop_dict[SOURCE] = 1
    marked_stop = deque()
    marked_stop.append(SOURCE)




    return label, star_label, marked_stop_dict, inf_time, marked_stop



def pareto_set(labels):
    """
    This function return pairwise non-dominating labels.

    Args:
        labels (nested list): list of different labels, which may not be pairwise non-dominating.

    Returns:
        pareto_list (nested list): contains list of pair wise non-dominating labels.

    """

    labels = copy.deepcopy(labels)
    is_efficient = np.ones(len(labels), dtype=bool)
    labels_criteria = np.array([x for x in labels])
    for i, label in enumerate(labels_criteria):
        if is_efficient[i]:
            is_efficient[is_efficient] = np.any(labels_criteria[is_efficient] < label, axis=1)
            is_efficient[i] = True

    new_labels = []
    for lab in labels_criteria[is_efficient]:
        lab = list(lab)  # converting each label to normal
        for i in range(1, len(lab)):
            lab[i] = int(lab[i])  # float has been changed to int
        new_labels.append(lab)

    pareto_list = [list(leb) for leb in new_labels]

    return pareto_list

def merge(existing_bag, new_bag, no_of_criteria):
    """
    This function perform the merging operation of two bag(nested list) such that none of the label dominates any other label and find the labels which are newly added due to the merger and assign -1 trip to those labels.

    Args:
        existing_bag (nested list): contain the existing labels associated with a stop.
        new_bag (nested list): contain new labels for that stop.
        no_of_criteria (int): number of criteria taken other than rounds.

    Returns:
        merged_bag_with_trips (nested list): contains labels which are pairwise non-dominating with associated trips and for all new labels which were not there in 'existing_bag' the associated trip as -1.
        newly_added_labels_with_trips_1 (nested list): contains labels which are newly added to the 'existing_bag' due to merger with trip as -1

    """

    # creating "existing_bag_dict" to preserve the associated trips for each label in existing_bag.
    existing_bag_dict = {tuple(lab) : tuple(lab[:-1]) for lab in existing_bag}

    # removing the associated trips from the labels of existing_bag.
    existing_bag_label_without_trip = [lab[:-1] for lab in existing_bag]

    existing_bag_without_duplicates = []
    for x in existing_bag_label_without_trip:
        if x not in existing_bag_without_duplicates:
            existing_bag_without_duplicates.append(x)

    # removing the associated trips from the labels of new_bag.
    new_bag_label_without_trip = [lab[:-1] for lab in new_bag]

    new_bag_without_duplicates = []
    for z in new_bag_label_without_trip:
        if z not in new_bag_without_duplicates:
            new_bag_without_duplicates.append(z)

    # adding all the labels without removing dominated labels into "extended_bag".
    extended_bag = existing_bag_without_duplicates + new_bag_without_duplicates

    extended_bag_without_duplicates = []
    for j in extended_bag:
        if j not in extended_bag_without_duplicates:
            extended_bag_without_duplicates.append(j)

    merged_bag = pareto_set(extended_bag_without_duplicates)

    # finding the newly added labels in existing_bag due to merger of bags.
    newly_added_labels = []
    for label in new_bag_without_duplicates:
        if (label in merged_bag) and (label not in existing_bag_without_duplicates):
            newly_added_labels.append(label)

    merged_bag_without_duplication = []
    for i in merged_bag:
        if i not in merged_bag_without_duplication:
            merged_bag_without_duplication.append(i)

    # assigning -1 trips to all newly added labels.
    newly_added_labels_with_trips_1 = []
    for z in merged_bag_without_duplication:
        if z in newly_added_labels:
            z.append(-1)
            newly_added_labels_with_trips_1.append(z)

    # assigning associated trips to label which were already in "existing_bag"
    merged_bag_with_trips = []
    for leb in merged_bag_without_duplication:
        for key in existing_bag_dict:
            if leb == list(existing_bag_dict[key]):
                merged_bag_with_trips.append(list(key))
                continue
        else:
            if len(leb) == no_of_criteria+1:
                merged_bag_with_trips.append(leb)

    return merged_bag_with_trips, newly_added_labels_with_trips_1

def check_non_dominance(label, bag, no_of_criteria):
    """
    This function check whether the label is dominated by any other label in the bag, if yes then return False, else return True.

    Args:
        label (list): list containing values for different criteria. Format-> [arrival_time, number_of_stops, IVTT, trip_id].
        bag (nested list): contains different labels for a stop. Format-> [label], where label = [arrival_time, number_of_stops, IVTT, trip_id]
        no_of_criteria (int): number of criteria taken other than rounds.

    Returns:
        False, if label is dominated by any other label in the bag, else return True.

    """

    label = label[:int(no_of_criteria)]
    bag = [x[:int(no_of_criteria)] for x in bag]

    all_list = [label] + bag
    non_dominating_labels = pareto_set(all_list)

    if (label in non_dominating_labels) and (label not in bag):
        return True
    else:
        return False

def give_non_dominating_labels(list_of_labels, no_of_criteria):
    """
    This function take list of labels and return list of those labels which are pairwise non-dominating to each other.

    Args:
        list_of_labels (nested list): contains labels for a stop, which may not pairwise non-dominating. Format-> [label], where label = [arrival_time, number_of_stops, IVTT, trip_id]
        no_of_criteria (int): number of criteria taken other than rounds.

    Returns:
        non_dominating_labels (nested list): list of labels which are pairwise non_dominating to each other.

    """
    list_of_labels_without_trip = [x[:no_of_criteria] for x in list_of_labels]
    list_of_label_without_duplicates = []
    for j in list_of_labels_without_trip:
        if j not in list_of_label_without_duplicates:
            list_of_label_without_duplicates.append(j)

    non_dominating_labels = pareto_set(list_of_label_without_duplicates)

    return non_dominating_labels

def get_latest_trip_new(route_id, stop_id, arrival_time, trips_in_route_dict, stops_in_trip_dict):
    """
    This function return latest trip after a certain time from the given stop of a route.

    Args:
        route_id (int): id of route
        stop_id (int): id of stop
        arrival_time (int): arrival time at stop in seconds.
        trips_in_route_dict (dict): keys: route ID, values: list of trips in the increasing order of start time. Format-> dict[route_ID] = [trip_1, trip_2].
        stops_in_trip_dict (dict): nested dictionary with primary key: trip_id and secondary key: stop_id with value: arrival time of that trip on that stop. Format-> {trip_id: {stop_id: arrival_time}}.

    Returns:
        if trip exists:
            trip ID
        else:
            -1

    """
    for trip_id in trips_in_route_dict[route_id]:
        if stops_in_trip_dict[trip_id][stop_id] >= arrival_time:
            return trip_id
    return -1

def IVTT(previous_stop, current_stop, stops_in_trip_dict, previous_stop_label):
    """
    This function calculate the in vehicle travel time for a stop based on the arrival time difference between the current stop and previous stop in a trip and adding that too ivtt incurred till the previous stop.

    Args:
        previous_stop (int): id of previous stop in that trip.
        current_stop (int): id of current stop in that trip.
        stops_in_trip_dict (dict): nested dictionary with primary key: trip_id and secondary key: stop_id with value: arrival time of that trip on that stop. Format-> {trip_id: {stop_id: arrival_time}}.
        previous_stop_label:

    Returns:
        ivtt_till_current_stop (int): In vehicle travel time incurred till the current stop on a particular trip in seconds.

    """
    trip_id = previous_stop_label[-1]
    ivtt_till_previous_stop = previous_stop_label[2]

    ivtt_till_current_stop = ivtt_till_previous_stop + stops_in_trip_dict[trip_id][current_stop] - stops_in_trip_dict[trip_id][previous_stop]

    return ivtt_till_current_stop
