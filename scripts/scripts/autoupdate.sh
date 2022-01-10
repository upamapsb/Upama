#!/bin/bash

set -e

BRANCH="master"
ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ../.. && pwd )"
SCRIPTS_DIR=$ROOT_DIR/scripts

has_changed() {
  git diff --name-only --exit-code $1 >/dev/null 2>&1
  [ $? -ne 0 ]
}

has_changed_gzip() {
  # Ignore the header because it includes the creation time
  cmp --silent -i 8 $1 <(git show HEAD:$1)
  [ $? -ne 0 ]
}

cd $ROOT_DIR

# Activate Python virtualenv
source $SCRIPTS_DIR/venv/bin/activate

# Interpret inline Python script in `scripts` directory
run_python() {
  (cd $SCRIPTS_DIR/scripts; python -c "$1")
}

# Make sure we have the latest commit.
git reset --hard origin/master
git pull

# =====================================================================
# JHU

# Attempt to download JHU CSVs
# run_python 'import jhu; jhu.download_csv()'
python -m cowidev.jhu download

# If there are any unstaged changes in the repo, then one of
# the CSVs has changed, and we need to run the update script.
if has_changed './scripts/input/jhu/*'; then
  echo "Generating JHU files..."
  python -m cowidev.jhu etl --skip-download
  # python $SCRIPTS_DIR/scripts/jhu.py --skip-download
  git add .
  git commit -m "data(jhu): automated update"
  git push
else
  echo "JHU export is up to date"
fi

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
python -m cowidev.jhu grapher-db
# run_python 'import jhu; jhu.update_db()'

# =====================================================================
# Policy responses

# The policy update files change far too often (every hour or so).
# We don't want to run an update if one has already been run in the
# last 6 hours.

OXCGRT_CSV_PATH=./scripts/input/bsg/latest.csv
UPDATE_INTERVAL_SECONDS=$(expr 60 \* 60 \* 24) # 24 hours
CURRENT_TIME=$(date +%s)
UPDATED_TIME=$(stat $OXCGRT_CSV_PATH -c %Y)

if [ $(expr $CURRENT_TIME - $UPDATED_TIME) -gt $UPDATE_INTERVAL_SECONDS ]; then
  # Download CSV
  python -m cowidev.oxcgrt etl
  # If there are any unstaged changes in the repo, then the
  # CSV has changed, and we need to run the update script.
  if has_changed $OXCGRT_CSV_PATH; then
    echo "Generating OxCGRT export..."
    python -m cowidev.oxcgrt grapher-file
    git add .
    git commit -m "data(oxcgrt): automated update"
    git push
  else
    echo "OxCGRT export is up to date"
  fi
else
  echo "OxCGRT CSV was recently updated; skipping download"
fi

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
# run_python 'import oxcgrt; oxcgrt.update_db()'
python -m cowidev.oxcgrt grapher-db

# =====================================================================
# US vaccinations

# # Attempt to download CDC data
# run_python 'import us_vaccinations; us_vaccinations.download_data()'

# # If there are any unstaged changes in the repo, then one of
# # the CSVs has changed, and we need to run the update script.
# echo "Generating US vaccination file..."
# run_python 'import us_vaccinations; us_vaccinations.generate_dataset()'
# if has_changed './public/data/vaccinations/us_state_vaccinations.csv'; then
#   git add .
#   git commit -m "Automated US vaccination update"
#   git push
#   run_python 'import us_vaccinations; us_vaccinations.update_db()'
# else
#   echo "US vaccination export is up to date"
# fi

echo "Generating US vaccination files..."
python -m cowidev.vax.us_states etl
python -m cowidev.vax.us_states grapher-file
if has_changed './public/data/vaccinations/us_state_vaccinations.csv'; then
  git add .
  git commit -m "data(us-vax): update"
  git push
else
  echo "US vaccination export is up to date"
fi

# =====================================================================
# Swedish Public Health Agency

# Attempt to download data
run_python 'import sweden; sweden.download_data()'

# If there are any unstaged changes in the repo, then one of
# the CSVs has changed, and we need to run the update script.
if has_changed './scripts/input/sweden/sweden_deaths_per_day.csv'; then
  echo "Generating Swedish Public Health Agency dataset..."
  run_python 'import sweden; sweden.generate_dataset()'
  git add .
  git commit -m "data(sweden): automated update"
  git push
else
  echo "Swedish Public Health Agency export is up to date"
fi

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
run_python 'import sweden; sweden.update_db()'

# =====================================================================
# Hospital & ICU data

hour=$(date +%H)
if [ $hour == 06 ] || [ $hour == 18 ] ; then
  # Download CSV
  echo "Generating hospital & ICU export..."
  python -m cowidev.hosp etl
  python -m cowidev.hosp grapher-file
  git add .
  git commit -m "data(hosp): automated update"
  git push
fi

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
python -m cowidev.hosp grapher-db

# =====================================================================
# UK subnational data

hour=$(date +%H)
if [ $hour == 17 ] ; then
  # Download CSV
  echo "Generating UK subnational export..."
  run_python 'import uk_nations; uk_nations.generate_dataset()'
  git add .
  git commit -m "data(uk): automated update"
  git push
fi

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
run_python 'import uk_nations; uk_nations.update_db()'

# =====================================================================
# Google Mobility

hour=$(date +%H)
if [ $hour == 15 ] ; then

  # Download CSV
  python -m cowidev.gmobility etl

  echo "Generating Google Mobility export..."
  python -m cowidev.gmobility grapher-file

  if has_changed './scripts/grapher/Google Mobility Trends (2020).csv'; then
    git add .
    git commit -m "data(mobility): automated update"
    git push
  fi

fi

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
python -m cowidev.gmobility grapher-db

# =====================================================================
# Variants
# If there are any unstaged changes in the repo, then one of
# the CSVs has changed, and we need to run the update script.
if [ $hour == 20 ] ; then
  echo "Generating CoVariants dataset..."
  python -m cowidev.variants etl
  python -m cowidev.variants grapher-file
  python -m cowidev.variants explorer-file
  git add .
  git commit -m "data(variants): automated update"
  git push
fi

# =====================================================================
# VAX ICER
# This basically download the vaccination data needed for some countries
# The idea is that here we put extremely slow scripts, so their updates are managed separately
hour=$(date +%H)
if [ $hour == 03 ] ; then
  echo "Generating ICE vaccination data..."
  python -m cowidev.vax.icer
fi

# =====================================================================
# Decoupling charts

hour=$(date +%H)
if [ $hour == 02 ] ; then
  echo "Generating decoupling dataset..."
  run_python 'import decoupling; decoupling.main()'
  git add .
  git commit -m "data(decoupling): automated update"
  git push
fi

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
run_python 'import decoupling; decoupling.update_db()'
