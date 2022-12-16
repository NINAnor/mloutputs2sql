#!/bin/bash

set -euo pipefail

BASE_FOLDER=$1
DB_NAME=$2
OUT_FOLDER=$(dirname $DB_NAME)
INDEX_LOCATION_FOLDER=$3

docker run --rm \
    -v $BASE_FOLDER:$BASE_FOLDER \
    -v $OUT_FOLDER:$OUT_FOLDER \
    --workdir $BASE_FOLDER \
    mloutput2sql \
    bash -c "find '$BASE_FOLDER' -type f -name '*.csv' | xargs \
    python /src/mloutput2sql.py --database_path $DB_NAME --index_location_folder $INDEX_LOCATION_FOLDER"
