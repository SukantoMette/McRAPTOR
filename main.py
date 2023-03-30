"""
This is the Main module.
"""

from Mcraptor import McRAPTOR
from Miscellenous_functions import read_testcase
from Miscellenous_functions import print_network_details
from Miscellenous_functions import print_query_parameter
from Miscellenous_functions import print_output

import time
import datetime as dt





def main():
    """
    Runs the McRAPTOR algorithm for the given Query parameters.

    """
    # Read network
    FOLDER = './swiss'

    stops_file, trips_file, stop_times_file, transfers_file, stops_dict, trips_in_route_dict, stops_in_trip_dict, footpath_dict, routes_by_stop_dict, idx_by_route_stop_dict = read_testcase(FOLDER)
    print_network_details(transfers_file, trips_file, stops_file)

    # Query parameters
    SOURCE = 20775
    DESTINATION = 1482
    DEPARTURE_TIME = "2019-06-10 00:00:00"
    DEPARTURE_TIME_IN_SEC = (dt.datetime.strptime(DEPARTURE_TIME, '%Y-%m-%d %H:%M:%S') - dt.datetime.strptime("1970-01-01 00:00:00", '%Y-%m-%d %H:%M:%S')).total_seconds()
    MAX_TRANSFER = 5
    NUMBER_OF_CRITERIA = 3

    print_query_parameter(SOURCE, DESTINATION, DEPARTURE_TIME, NUMBER_OF_CRITERIA, MAX_TRANSFER)
    start_time = time.time()
    final_label, inf_time = McRAPTOR(SOURCE, DESTINATION, DEPARTURE_TIME_IN_SEC, trips_in_route_dict, stops_in_trip_dict, routes_by_stop_dict, stops_dict, stops_file, footpath_dict, NUMBER_OF_CRITERIA,idx_by_route_stop_dict ,MAX_TRANSFER)
    last_time = time.time()
    print_output(final_label, DESTINATION, start_time, last_time, inf_time, MAX_TRANSFER)



if __name__ == "__main__":
    main()