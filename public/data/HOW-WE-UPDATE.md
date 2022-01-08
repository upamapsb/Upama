## How do we update our dataset?
We share the complete dataset as [CSV](https://covid.ourworldindata.org/data/owid-covid-data.csv),
[JSON](https://covid.ourworldindata.org/data/owid-covid-data.json)
and [XLSX](https://covid.ourworldindata.org/data/owid-covid-data.xlsx) files. This dataset contains several metrics, ranging
from vaccination to hospitalization.

We create it by first running several _sub-processes_ that generate intermidiate datasets and then jointly processing and merging all these intermediate datasets into a final and complete dataset.  

Consequently, the dataset is updated multiple times a day, using the currently available intermediate datasets. That is, for the vaccination data to be updated in our complete dataset, the vaccination intermediate dataset needs first to be updated.



### Our sub-processes
Find below a diagram with the different sub-processes, their approximate update frequency and intermediate generated datasets.

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
  │ Vaccination                                              │          │
  │                                                          │          │
  │  module: <a href="../../scripts/src/cowidev/vax/__main__.py">cowidev.vax</a>                                     │          │
  │  update: 12h UTC                                         │          │
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
  │ Hospitalization & ICU                                    │          │
  │                                                          │          │
  │  module: <a href="../../scripts/src/cowidev/hosp/__main__.py">cowidev.hosp</a>                                    │          │
  │  update: 6h and 18h UTC                                  │          │
  │                                                          │          │
  │           ┌───┐     ┌────────────┐     ┌──────────┐      │          │
  │  steps:   │<a href="../../scripts/src/cowidev/hosp/etl.py">etl</a>├────►│<a href="../../scripts/src/cowidev/hosp/grapher.py">grapher-file</a>├────►│<a href="../../scripts/src/cowidev/hosp/grapher.py">grapher-db</a>│      │          │
  │           └───┘     └────────────┘     └──────────┘      │          │
  │                                                          │          │
  │                                                          │          │
  │  output:  <a href="hospitalizations/covid-hospitalizations.csv">covid-hospitalizations.csv</a> ──────────────────── ──────────┤
  │                                                          │          │
  └──────────────────────────────────────────────────────────┘          │
                                                                        │          ┌───────────────────────────────┐
  ┌──────────────────────────────────────────────────────────┐          │          │ Megafile                      │
  │ Testing                                                  │          │          │                               │
  │                                                          │          │          │  module: <a href="../../scripts/src/cowidev/megafile/__main__.py">cowidev.megafile</a>     │
  │  module: <a href="../../scripts/scripts/testing/">scripts/scripts/testing/</a>                        │          ├─────────►│  update: 6h, 18h UTC          │
  │  update: every day                                       │          │          │                               │
  │                                                          │          │          │  output:  owid-covid-data.csv │
  │           ┌─────────────────┐     ┌────────────────┐     │          │          │                               │
  │  steps:   │<a href="../../scripts/scripts/testing/run_python_scripts.py">run_python_script</a>├────►│<a href="../../scripts/scripts/testing/generate_dataset.R">generate_dataset</a>│     │          │          └───────────────────────────────┘
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
  │  update: every 24 hours                                  │          │
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
  │ Variants                                                 │          │
  │                                                          │          │
  │  module: <a href="../../scripts/src/cowidev/variants/__main__.py">cowidev.variants</a>                                │          │
  │  update: 20h UTC                                         │          │
  │                                                          │          │
  │           ┌───┐     ┌────────────┐     ┌─────────────┐   │          │
  │  steps:   │<a href="../../scripts/src/cowidev/variants/etl.py">etl</a>├────►│<a href="../../scripts/src/cowidev/variants/grapher.py">grapher-file</a>├────►│<a href="../../scripts/src/cowidev/variants/grapher.py">explorer-file</a>│   │          │
  │           └───┘     └────────────┘     └─────────────┘   │          │
  │                                                          │          │
  │                                                          │          │
  │  output:  <a href="variants/covid-variants.csv">covid-variants.csv</a> ──────────────────────────── ──────────┤
  │           <a href="variants/covid-sequencing.csv">covid-sequencing.csv</a>                           │          │
  └──────────────────────────────────────────────────────────┘          │
                                                                        │
  ┌──────────────────────────────────────────────────────────┐          │
  │ Excess Mortality                                         │          │
  │                                                          │          │
  │  module: <a href="../../scripts/src/cowidev/xm/__main__.py">cowidev.xm</a>                                      │          │
  │  update: 6h and 18h UTC                                  │          │
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


