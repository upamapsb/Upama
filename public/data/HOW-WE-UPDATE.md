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
  │  steps:   │etl├───►│grapher-file│                        │
  │           └───┘    └────────────┘                        │
  │                                                          │
  │                                                          │
  │  output:  jhu/─────────────────────────────────────────── ──────────┐
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
  │  steps:   │get├────►│process├────►│generate├───►│export│ │          │
  │           └───┘     └───────┘     └────────┘    └──────┘ │          │
  │                                                          │          │
  │                                                          │          │
  │  output:  vaccinations.csv ────────────────────────────── ──────────│
  │           vaccinations-by-manufacturer.csv               │          │
  │           vaccinations-by-age-group.csv                  │          │
  └──────────────────────────────────────────────────────────┘          │
                                                                        │
  ┌──────────────────────────────────────────────────────────┐          │
  │ Hospitalization & ICU                                    │          │
  │                                                          │          │
  │  module: <a href="../../scripts/src/cowidev/hosp/__main__.py">cowidev.hosp</a>                                    │          │
  │  update: 6h and 18h UTC                                  │          │
  │                                                          │          │
  │           ┌───┐     ┌────────────┐     ┌──────────┐      │          │
  │  steps:   │etl├────►│grapher-file├────►│grapher-db│      │          │
  │           └───┘     └────────────┘     └──────────┘      │          │
  │                                                          │          │
  │                                                          │          │
  │  output:  covid-hospitalizations.csv ──────────────────── ──────────┤
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
  │  steps:   │run_python_script├────►│generate_dataset│     │          │          └───────────────────────────────┘
  │           │run_R_scripts    │     └────────────────┘     │          │
  │           └─────────────────┘                            │          │
  │                                                          │          │
  │  output:  covid-testing-all-observations.csv ──────────── ──────────┤
  │                                                          │          │
  └──────────────────────────────────────────────────────────┘          │
                                                                        │
  ┌──────────────────────────────────────────────────────────┐          │
  │ Policy responses (OXCGRT)                                │          │
  │                                                          │          │
  │  module: <a href="../../scripts/src/cowidev/oxcgrt/__main__.py">cowidev.oxcgrt</a>                                  │          │
  │  update: every 24 hours                                  │          │
  │                                                          │          │
  │           ┌───┐    ┌────────────┐    ┌──────────┐        │          │
  │  steps:   │etl├───►│grapher-file├───►│grapher-db│        │          │
  │           └───┘    └────────────┘    └──────────┘        │          │
  │                                                          │          │
  │                                                          │          │
  │  output:  latest.csv ──────────────────────────────────── ──────────┤
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
  │  steps:   │etl├────►│grapher-file├────►│explorer-file│   │          │
  │           └───┘     └────────────┘     └─────────────┘   │          │
  │                                                          │          │
  │                                                          │          │
  │  output:  covid-variants.csv ──────────────────────────── ──────────┤
  │           covid-sequencing.csv                           │          │
  └──────────────────────────────────────────────────────────┘          │
                                                                        │
  ┌──────────────────────────────────────────────────────────┐          │
  │ Excess Mortality                                         │          │
  │                                                          │          │
  │  module: <a href="../../scripts/src/cowidev/xm/__main__.py">cowidev.xm</a>                                      │          │
  │  update: 6h and 18h UTC                                  │          │
  │                                                          │          │
  │           ┌───┐                                          │          │
  │  steps:   │etl│                                          │          │
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
  │  <a href="https://github.com/crondonm/TrackingR/raw/main/Estimates-Database/database.csv">remote file</a> ──────────────────────────────────────────── ──────────┘
  │                                                          │
  └──────────────────────────────────────────────────────────┘
</pre>


