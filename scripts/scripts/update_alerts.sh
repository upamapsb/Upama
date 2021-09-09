#!/bin/bash
set -e
cd /home/owid/covid-19-data/scripts
source venv/bin/activate

hour=$(date +%H)

if [ $hour == 7 ] ; then
  python scripts/update_alerts.py --data jhu
fi

if [ $hour == 11 ] ; then
  python scripts/update_alerts.py --data vax
fi

if [ $hour == 15 ] ; then
  python scripts/update_alerts.py --data testing
fi
