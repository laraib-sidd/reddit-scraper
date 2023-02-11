import argparse
from utils import *


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n",
                        "--name",
                        type=str,
                        help="subredit_name",
                        dest="sub_name",
                        required=True)
    parser.add_argument("-sd", "--start-date", type=str,
                        help="start_date format YYYY/MM/DD example : 2021-02-01", dest="start_date", required=True)
    parser.add_argument("-ed", "--end-date", type=str,
                        help="end_date format YYYY/MM/DD example : 2022-04-01", dest="end_date", required=True)
    parser.add_argument("-l", "--load-type", type=str,
                        help="output file, options 1)mongo 2)csv 3)dual", dest="output", required=True)
    args = parser.parse_args()

    class Object(object):
        pass

    temp_args = Object()
    temp_args.sub_name = "btc"
    temp_args.start_date = "2022-11-05"
    temp_args.end_date = "2022-11-06"
    temp_args.load_type = "mongo"

    validate_args(args)

    sub_exists(args)
    date_list = get_date_list(args)
    load_type = temp_args.load_type

    mongo_client = get_mongo_client()

    if load_type == "csv":
        load_csv_posts_api(date_list, args)
    if load_type == "mongo":
        load_mongo_posts_api(date_list, mongo_client, args)
    if load_type == "dual":
        load_csv_posts_api(date_list, args)
        load_mongo_posts_api(date_list, mongo_client, args)


if __name__ == "__main__":
    # pass
    main()
