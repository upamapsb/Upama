# How do we update our dataset?

We share the complete dataset as [CSV](https://covid.ourworldindata.org/data/owid-covid-data.csv),
[JSON](https://covid.ourworldindata.org/data/owid-covid-data.json)
and [XLSX](https://covid.ourworldindata.org/data/owid-covid-data.xlsx) files. This dataset contains many metrics. More details about the dataset can be found [here](https://github.com/owid/covid-19-data/tree/master/public/data).

We produce this dataset by

1. Running several _sub-processes_ that generate intermediate datasets.
2. Jointly processing and merging all these intermediate datasets into the final and complete dataset.  

Consequently, the dataset is updated multiple times a day (_at least_ at 06:00 and 18:00 UTC), using the latest generated intermediate datasets.


## Dataset sub-processes

Find below a diagram with the different sub-processes, their approximate update frequency and intermediate generated
datasets. This diagram only shows the sub-processes relevant for the production of the complete dataset, as there are
other sub-processes producing data that may appear on our website (Grapher) but that is not present in the complete dataset.

<pre>
  ┌──────────────────────────────────────────────────────────┐
  │ Cases & Deaths (JHU)                                     │
  │                                                          │
  │  module: <a href="../../scripts/src/cowidev/jhu/__main__.py">cowidev.jhu</a>                                     │
  │  update: every hour (if new data)                        │
  │                                                          │
  │           ┌───┐    ┌────────────┐                        │
  │  steps:   │<a href="../../scripts/src/cowidev/jhu/__main__.py">etl</a>├───►│<a href="../../scripts/src/cowidev/jhu/__main__.py">grapher-file</a>│                        │
  │           └───┘    └────────────┘                        │
  │                                                          │
  │                                                          │
  │  output:  <a href="jhu/">jhu/</a>─────────────────────────────────────────── ──────────┐
  │                                                          │          │
  └──────────────────────────────────────────────────────────┘          │
                                                                        │
  ┌──────────────────────────────────────────────────────────┐          │
  │ <a href="vaccination/">Vaccination</a>                                              │          │
  │                                                          │          │
  │  module: <a href="../../scripts/src/cowidev/vax/__main__.py">cowidev.vax</a>                                     │          │
  │  update: daily at 12:00 UTC                              │          │
  │                                                          │          │
  │           ┌───┐     ┌───────┐     ┌────────┐    ┌──────┐ │          │
  │  steps:   │<a href="../../scripts/src/cowidev/vax/cmd/get_data.py">get</a>├────►│<a href="../../scripts/src/cowidev/vax/cmd/process_data.py">process</a>├────►│<a href="../../scripts/src/cowidev/vax/cmd/generate_dataset.py">generate</a>├───►│<a href="../../scripts/src/cowidev/vax/cmd/export.py">export</a>│ │          │
  │           └───┘     └───────┘     └────────┘    └──────┘ │          │
  │                                                          │          │
  │                                                          │          │
  │  output:  <a href="vaccinations/vaccinations.csv">vaccinations.csv</a> ────────────────────────────── ──────────│
  │           <a href="vaccinations/vaccinations-by-manufacturer.csv">vaccinations-by-manufacturer.csv</a>               │          │
  │           <a href="vaccinations/vaccinations-by-age-group.csv">vaccinations-by-age-group.csv</a>                  │          │
  └──────────────────────────────────────────────────────────┘          │
                                                                        │
  ┌──────────────────────────────────────────────────────────┐          │
  │ <a href="hospitalizations/">Hospitalization & ICU</a>                                    │          │
  │                                                          │          │
  │  module: <a href="../../scripts/src/cowidev/hosp/__main__.py">cowidev.hosp</a>                                    │          │
  │  update: daily at 06:00 and 18:00 UTC                    │          │
  │                                                          │          │
  │           ┌───┐     ┌────────────┐     ┌──────────┐      │          │
  │  steps:   │<a href="../../scripts/src/cowidev/hosp/etl.py">etl</a>├────►│<a href="../../scripts/src/cowidev/hosp/grapher.py">grapher-file</a>├────►│<a href="../../scripts/src/cowidev/hosp/grapher.py">grapher-db</a>│      │          │
  │           └───┘     └────────────┘     └──────────┘      │          │
  │                                                          │          │
  │                                                          │          │
  │  output:  <a href="hospitalizations/covid-hospitalizations.csv">covid-hospitalizations.csv</a> ──────────────────── ──────────┤
  │                                                          │          │
  └──────────────────────────────────────────────────────────┘          │
                                                                        │      ┌──────────────────────────────────┐
  ┌──────────────────────────────────────────────────────────┐          │      │ Megafile                         │
  │ Testing                                                  │          │      │                                  │
  │                                                          │          │      │  module: <a href="../../scripts/src/cowidev/megafile/__main__.py">cowidev.megafile</a>        │
  │  module: <a href="../../scripts/scripts/testing/">scripts/scripts/testing/</a>                        │          ├─────►│  update: daily at 6h and 18h UTC │
  │  update: 3 times per week                                │          │      │                                  │
  │                                                          │          │      │  output:  <a href="owid-covid-data.csv">owid-covid-data.csv</a>    │
  │           ┌─────────────────┐     ┌────────────────┐     │          │      │                                  │
  │  steps:   │<a href="../../scripts/scripts/testing/run_python_scripts.py">run_python_script</a>├────►│<a href="../../scripts/scripts/testing/generate_dataset.R">generate_dataset</a>│     │          │      └──────────────────────────────────┘
  │           │<a href="../../scripts/scripts/testing/run_r_scripts.R">run_R_scripts</a>    │     └────────────────┘     │          │
  │           └─────────────────┘                            │          │
  │                                                          │          │
  │  output:  <a href="testing/covid-testing-all-observations.csv">covid-testing-all-observations.csv</a> ──────────── ──────────┤
  │                                                          │          │
  └──────────────────────────────────────────────────────────┘          │
                                                                        │
  ┌──────────────────────────────────────────────────────────┐          │
  │ Policy responses (OxCGRT)                                │          │
  │                                                          │          │
  │  module: <a href="../../scripts/src/cowidev/oxcgrt/__main__.py">cowidev.oxcgrt</a>                                  │          │
  │  update: daily                                           │          │
  │                                                          │          │
  │           ┌───┐    ┌────────────┐    ┌──────────┐        │          │
  │  steps:   │<a href="../../scripts/src/cowidev/oxcgrt/etl.py">etl</a>├───►│<a href="../../scripts/src/cowidev/oxcgrt/grapher.py">grapher-file</a>├───►│<a href="../../scripts/src/cowidev/oxcgrt/grapher.py">grapher-db</a>│        │          │
  │           └───┘    └────────────┘    └──────────┘        │          │
  │                                                          │          │
  │                                                          │          │
  │  output:  <a href="../../scripts/input/bsg/latest.csv">latest.csv</a> ──────────────────────────────────── ──────────┤
  │                                                          │          │
  └──────────────────────────────────────────────────────────┘          │
                                                                        │
  ┌──────────────────────────────────────────────────────────┐          │
  │ <a href="variants/">Variants</a>                                                 │          │
  │                                                          │          │
  │  module: <a href="../../scripts/src/cowidev/variants/__main__.py">cowidev.variants</a>                                │          │
  │  update: daily at 20:00 UTC                              │          │
  │                                                          │          │
  │           ┌───┐     ┌────────────┐     ┌─────────────┐   │          │
  │  steps:   │<a href="../../scripts/src/cowidev/variants/etl.py">etl</a>├────►│<a href="../../scripts/src/cowidev/variants/grapher.py">grapher-file</a>├────►│<a href="../../scripts/src/cowidev/variants/grapher.py">explorer-file</a>│   │          │
  │           └───┘     └────────────┘     └─────────────┘   │          │
  │                                                          │          │
  │                                                          │          │
  │  output:  covid-variants.csv ──────────────────────────── ──────────┤
  │           covid-sequencing.csv                           │          │
  └──────────────────────────────────────────────────────────┘          │
                                                                        │
  ┌──────────────────────────────────────────────────────────┐          │
  │ <a href="excess_mortality/">Excess mortality</a>                                         │          │
  │                                                          │          │
  │  module: <a href="../../scripts/src/cowidev/xm/__main__.py">cowidev.xm</a>                                      │          │
  │  update: daily at 06:00 and 18:00 UTC                    │          │
  │                                                          │          │
  │           ┌───┐                                          │          │
  │  steps:   │<a href="../../scripts/src/cowidev/xm/etl.py">etl</a>│                                          │          │
  │           └───┘                                          │          │
  │                                                          │          │
  │                                                          │          │
  │  output:  <a href="excess_mortality/excess_mortality.csv">excess_mortality.csv</a> ────────────────────────── ──────────┤
  │           <a href="excess_mortality/excess_mortality_economist_estimates.csv">excess_mortality_economist_estimates.csv</a> ────── ──────────┤
  │                                                          │          │
  └──────────────────────────────────────────────────────────┘          │
                                                                        │
  ┌──────────────────────────────────────────────────────────┐          │
  │ Reproduction rate                                        │          │
  │                                                          │          │
  │  <a href="https://github.com/crondonm/TrackingR/blob/main/Estimates-Database/database.csv">remote file</a> ──────────────────────────────────────────── ──────────┘
  │                                                          │
  └──────────────────────────────────────────────────────────┘
</pre>


## Other subprocesses

The following sub-processes generate other intermediate datasets relevant for our Grapher and Explorer charts (their
metrics are not present in the compelete dataset).

<pre>
  ┌──────────────────────────────────────────────────────────┐
  │ <a href="vaccination/">Vaccination US States</a>                                    │
  │                                                          │
  │  module: <a href="../../scripts/src/cowidev/vax/us_states/__main__.py">cowidev.vax.us_states</a>                           │
  │  update: every hour                                      │
  │                                                          │
  │           ┌───┐     ┌────────────┐                       │
  │  steps:   │<a href="../../scripts/src/cowidev/vax/us_states/etl.py">etl</a>├────►│<a href="../../scripts/src/cowidev/vax/us_states/grapher.py">grapher-file</a>│                       │
  │           └───┘     └────────────┘                       │
  │                                                          │
  │                                                          │
  │  output:  <a href="vaccinations/us_state_vaccinations.csv">us_state_vaccinations.csv</a>                      │
  │                                                          │
  └──────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────┐
  │ <a href="vaccination/">Local UK Data</a>                                            │
  │                                                          │
  │  module: <a href="../../scripts/scripts/uk_nations.py">uk_nations.py</a>                                   │
  │  update: daily at 17:00 UTC                              │
  │                                                          │
  │           ┌────────────────┐    ┌─────────┐              │
  │  steps:   │<a href="../../scripts/scripts/uk_nations.py">generate_dataset</a>├───►│<a href="../../scripts/scripts/uk_nations.py">update_db</a>│              │
  │           └────────────────┘    └─────────┘              │
  │                                                          │
  │                                                          │
  │  output:  <a href="../../scripts/grapher/uk_covid_data.csv">uk_covid_data.csv</a>                              │
  │                                                          │
  └──────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────┐
  │ Google Mobility                                          │
  │                                                          │
  │  module: <a href="../../scripts/src/cowidev/gmobility/__main__.py">cowidev.gmobility</a>                               │
  │  update: daily at 15:00 UTC                              │
  │                                                          │
  │           ┌───┐     ┌────────────┐                       │
  │  steps:   │<a href="../../scripts/src/cowidev/gmobility/etl.py">etl</a>├────►│<a href="../../scripts/src/cowidev/gmobility/grapher.py">grapher-file</a>│                       │
  │           └───┘     └────────────┘                       │
  │                                                          │
  │                                                          │
  │  output:  <a href="../../scripts/grapher/Google Mobility Trends (2020).csv">Google Mobility Trends (2020).csv</a>              │
  │                                                          │
  └──────────────────────────────────────────────────────────┘

</pre>
