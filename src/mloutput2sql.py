import argparse
import pandas
import re
import datetime
import sqlalchemy

from tqdm import tqdm

recorder_filename_date = re.compile(r"\d{8}_\d{6}.csv")

def filename_to_datetime(filename):
    matches = recorder_filename_date.search(filename)
    if not matches:
        return  # Invalid filename
    try:
        dt = datetime.datetime.strptime(matches.group(0), "%Y%m%d_%H%M%S.csv")
    except ValueError:
        return  # Wrong format
    return dt
    
def add_location(item, filename, index_location_folder):
    location = filename.split("/")[index_location_folder]
    item["location"] = location
    return item
        
def add_prefix(item, filename):
    prefix = filename.split('/')[-1].split('_')[0]
    item["prefix"] = prefix
    return item
        
def add_filename(item, filename):
    file_name = filename.split('/')[-1].split('.')[0]
    item["filename"] = file_name
    return item   
    
def add_date(item, dt):
    dt_ymd = dt.strftime('%Y-%m-%d')
    item["date"] = dt_ymd
    return item
    
def add_time_detection(item, dt):

    item["time_detection"] = [dt + datetime.timedelta(seconds=s) for s in item["start_detection"]]
    item["time_detection"] = [i.strftime('%H:%M:%S') for i in item["time_detection"]]
    return item
    
def add_info(filename, parsed, prefix, index_location_folder, dt):

    improved = add_date(parsed, dt)
    improved = add_location(parsed, filename, index_location_folder)
    improved = add_time_detection(parsed, dt)
    if prefix:
        improved = add_prefix(parsed, filename)
    improved = add_filename(parsed, filename)
    return improved

def main(database_path, recreate, results, prefix, index_location_folder):

    db = sqlalchemy.create_engine('sqlite:///{}'.format(database_path))

    for result in tqdm(results):
        try:
            dt = filename_to_datetime(result)
            parsed = pandas.read_csv(result)    
            improved = add_info(result, parsed, prefix, index_location_folder, dt)
            improved.to_sql(database_path, db, if_exists="append")
        except:
            print(f"Could not analyse {result}")
            continue
        
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
        description="Convert BirdNet results into a SQLite database",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--database_path",
        help="Path of the database to create or update",
        default="common.sqlite",
    )
    parser.add_argument(
        "--prefix",
        help="Does the file name has a prefix before HMS_YMD",
        default=False,
    )
    parser.add_argument(
        "--index_location_folder",
        help="Does the file name has a prefix before HMS_YMD",
        default=-1,
        type=int
    )
    parser.add_argument(
        "--recreate",
        help="Recreate the database",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "results",
        nargs="+",
        help="BirdNet result file",
    )
    args = parser.parse_args()
    main(args.database_path, args.recreate, args.results, args.prefix, args.index_location_folder)

