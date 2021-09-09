#!/bin/bash
set -e
cd /home/owid/covid-19-data/scripts
source venv/bin/activate

hour=$(TZ=Europe/Paris date +%H)

if [ $hour == 7 ] ; then
  python scripts/update_alerts.py --data jhu
fi

if [ $hour == 12 ] ; then
  python scripts/update_alerts.py --data vax
fi

if [ $hour == 14 ] ; then
  python scripts/update_alerts.py --data testing
fi
