#! /bin/bash

get_and_unzip () {
    local URL=$1
    local TYPE=$2
    local FILENAME=$3
    wget -P "data/${TYPE}" -q --show-progress $URL
    gzip -d "data/${TYPE}/${FILENAME}"
}

# States without 2020 OD data: AK, AR, MS, PR
STATES="ak al ar az ca co ct dc de fl ga hi ia id il in ks ky la ma md me mi mn mo ms mt 
nc nd ne nh nj nm nv ny oh ok or pa pr ri sc sd tn tx ut va vt wa wi wv wy"

# OD data https://lehd.ces.census.gov/data/lodes/LODES8/${state}/od/${state}_od_main_JT00_2020.csv.gz
START="$(date +"%T")"
for state in $STATES
do
    RAC_URL="https://lehd.ces.census.gov/data/lodes/LODES8/${state}/rac/${state}_rac_S000_JT01_2020.csv.gz"
    get_and_unzip $RAC_URL "rac" "${state}_rac_S000_JT01_2020.csv"
    WAC_URL="https://lehd.ces.census.gov/data/lodes/LODES8/${state}/wac/${state}_wac_S000_JT01_2020.csv.gz"
    get_and_unzip $WAC_URL "wac" "${state}_wac_S000_JT01_2020.csv"
    GEO_URL="https://lehd.ces.census.gov/data/lodes/LODES8/${state}/${state}_xwalk.csv.gz"
    get_and_unzip $GEO_URL "geo" "${state}_xwalk.csv"
done
END="$(date +"%T")"
echo "Started at: $START"
echo "Ended at: $END"
