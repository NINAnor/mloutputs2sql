import argparse
import pandas
import re
import datetime
import sqlalchemy

from tqdm import tqdm

recorder_filename_date = re.compile(r"(?:\d{8}_\d{6}.csv)|(?:\d{8}-\d{6}.csv)|(?:\d{14}.csv)|(?:\d{8}.csv)|(?:\d{4}-\d{2}-\d{2}_\d{6}.csv)")

def aggregate(item):

    item['continuation'] = (item.filename == item.filename.shift()) & (item.end_detection.shift() == item.start_detection) & (item.confidence > 0.95) & (item.hr > 0.05)
    item['keep'] = ~item.continuation.shift(-1).astype(bool)
    item.at[item.index[-1], 'keep'] = True
    item.start_detection = item.start_detection[~item.continuation]
    item.start_detection = item.start_detection.fillna(method="ffill").astype(int)
    item["start_shift"] = item["start_detection"].shift()
    item["cumsum"] = (item["start_detection"] != item["start_shift"]).cumsum()

    # Aggregate conference
    item["conf_agg"] = item.groupby("cumsum")['confidence'].transform('mean')

    # Aggregate HR
    item["hr_agg"] = item.groupby("cumsum")['hr'].transform('mean')

    # Keep what we need
    item_agg = (item[item.keep]
        .assign(duration=item.end_detection - item.start_detection)
        .drop(columns=['continuation', 'keep', 'confidence', 'hr', 'cumsum', 'start_shift']).reset_index(drop=True))

    return item_agg

def filename_to_datetime(filename):
    matches = recorder_filename_date.search(filename)

    if not matches:
        return  # Invalid filename
    if bool(re.search(r'\d{8}_\d{6}.csv', matches.group(0))):
        dt = datetime.datetime.strptime(matches.group(0), "%Y%m%d_%H%M%S.csv")    
    if bool(re.search(r'\d{8}-\d{6}.csv', matches.group(0))):
        dt = datetime.datetime.strptime(matches.group(0), "%Y%m%d-%H%M%S.csv")
    if bool(re.search(r'\d{14}.csv', matches.group(0))):
        dt = datetime.datetime.strptime(matches.group(0), "%Y%m%d%H%M%S.csv")
    if bool(re.search(r'\d{8}.csv', matches.group(0))):
        dt = datetime.datetime.strptime(matches.group(0), "%Y%m%d.csv")
    if bool(re.search(r'\d{4}-\d{2}-\d{2}_\d{6}.csv', matches.group(0))):
        dt = datetime.datetime.strptime(matches.group(0), "%Y-%m-%d_%H%M%S.csv")
    return dt
    
def add_location(item, filename, index_location_folder):
    location = filename.split("/")[index_location_folder]
    item["location"] = location
    return item
        
def add_filename(item, filename):
    item["filename"] = filename
    return item   
    
def add_date(item, dt):
    dt_ymd = dt.strftime('%Y-%m-%d')
    item["date"] = dt_ymd
    return item
    
def add_time_detection(item, dt):
    item["time_detection"] = [dt + datetime.timedelta(seconds=s) for s in item["start_detection"]]
    item["time_detection"] = [i.strftime('%H:%M:%S') for i in item["time_detection"]]
    return item
    
def add_info(filename, parsed, index_location_folder, dt):

    improved = add_filename(parsed, filename)
    improved = aggregate(improved)
    improved = add_date(improved, dt)
    improved = add_location(improved, filename, index_location_folder)
    improved = add_time_detection(improved, dt)
    return improved

def main(database_path, recreate, results, index_location_folder):

    db = sqlalchemy.create_engine('sqlite:///{}'.format(database_path))

    for result in tqdm(results):
        print(result)
        dt = filename_to_datetime(result)
        parsed = pandas.read_csv(result) 
        if parsed.shape[0] > 0:  
            improved = add_info(result, parsed, index_location_folder, dt)
            improved.to_sql(database_path, db, if_exists="append")
        else:
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
    main(args.database_path, args.recreate, args.results, args.index_location_folder)

