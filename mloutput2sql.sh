#!/bin/bash

set -euo pipefail

BASE_FOLDER=$1
DB_NAME=$2
OUT_FOLDER=$(dirname $DB_NAME)
IS_PREFIX=$3
INDEX_LOCATION_FOLDER=$4

docker run --rm \
    -v $BASE_FOLDER:$BASE_FOLDER \
    -v $OUT_FOLDER:$OUT_FOLDER \
    --workdir $BASE_FOLDER \
    mloutput2sql \
    bash -c "find $BASE_FOLDER -type f -name '*.csv' | xargs \
    python /src/mloutput2sql.py --database_path $DB_NAME --prefix $IS_PREFIX --index_location_folder $INDEX_LOCATION_FOLDER"
