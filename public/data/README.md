# Data on COVID-19 (coronavirus) by _Our World in Data_


### 🗂️ Download our complete COVID-19 dataset : [CSV](https://covid.ourworldindata.org/data/owid-covid-data.csv) | [XLSX](https://covid.ourworldindata.org/data/owid-covid-data.xlsx) | [JSON](https://covid.ourworldindata.org/data/owid-covid-data.json)

Our complete COVID-19 dataset is a collection of the COVID-19 data maintained by [_Our World in Data_](https://ourworldindata.org/coronavirus). We will update it daily throughout the duration of the COVID-19 pandemic. It includes the following data:

| Metrics                     | Source                                                    | Updated | Countries |
|-----------------------------|-----------------------------------------------------------|---------|-----------|
| Vaccinations                | Official data collated by the Our World in Data team      | Daily   | 217       |
| Tests & positivity          | Official data collated by the Our World in Data team      | Weekly  | 139       |
| Hospital & ICU              | Official data collated by the Our World in Data team      | Weekly  | 37        |
| Confirmed cases             | JHU CSSE COVID-19 Data                                    | Daily   | 194        |
| Confirmed deaths            | JHU CSSE COVID-19 Data                                    | Daily   | 194       |
| Reproduction rate           | Arroyo-Marioli F, Bullano F, Kucinskas S, Rondón-Moreno C | Daily   | 185        |
| Policy responses            | Oxford COVID-19 Government Response Tracker               | Daily   | 186        |
| Other variables of interest | International organizations (UN, World Bank, OECD, IHME…) | Fixed   | 241       |

A [specific section of this repository](https://github.com/owid/covid-19-data/tree/master/public/data/vaccinations) is also dedicated to **vaccinations**, with a lighter dataset containing only vaccination data.


## The data you find here and our data sources

- **Confirmed cases and deaths:** our data comes from the [COVID-19 Data Repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University](https://github.com/CSSEGISandData/COVID-19) (JHU). We discuss how and when JHU collects and publishes this data [here](https://ourworldindata.org/coronavirus-source-data). The cases & deaths dataset is updated daily. *Note: the number of cases or deaths reported by any institution—including JHU, the WHO, the ECDC and others—on a given day does not necessarily represent the actual number on that date. This is because of the long reporting chain that exists between a new case/death and its inclusion in statistics. **This also means that negative values in cases and deaths can sometimes appear when a country corrects historical data, because it had previously overestimated the number of cases/deaths. Alternatively, large changes can sometimes (although rarely) be made to a country's entire time series if JHU decides (and has access to the necessary data) to correct values retrospectively.***
- **Hospitalizations and intensive care unit (ICU) admissions:** our data comes from the [European Centre for Disease Prevention and Control](https://www.ecdc.europa.eu/en/publications-data/download-data-hospital-and-icu-admission-rates-and-current-occupancy-covid-19) (ECDC) for a select number of European countries; and government sources for the [United Kingdom](https://coronavirus.data.gov.uk/details/healthcare), the [United States](https://healthdata.gov/Hospital/COVID-19-Reported-Patient-Impact-and-Hospital-Capa/g62h-syeh), [Canada](https://covid19tracker.ca/), [Israel](https://datadashboard.health.gov.il/COVID-19/general), [Algeria](https://github.com/yasserkaddour/covid19-icu-data-algeria), and [Switzerland](https://opendata.swiss/fr/dataset/covid-19-schweiz).
- **Testing for COVID-19:** this data is collected by the _Our World in Data_ team from official reports; you can find further details in our post on COVID-19 testing, including our [checklist of questions to understand testing data](https://ourworldindata.org/coronavirus-testing#our-checklist-for-covid-19-testing-data), information on [geographical and temporal coverage](https://ourworldindata.org/coronavirus-testing#which-countries-do-we-have-testing-data-for), and [detailed country-by-country source information](https://ourworldindata.org/coronavirus-testing#source-information-country-by-country). The testing dataset is updated around twice a week.
- **Vaccinations against COVID-19:** this data is collected by the _Our World in Data_ team from official reports.
- **Other variables:** this data is collected from a variety of sources (United Nations, World Bank, Global Burden of Disease, Blavatnik School of Government, etc.). More information is available in [our codebook](https://github.com/owid/covid-19-data/tree/master/public/data/owid-covid-codebook.csv).


## The complete _Our World in Data_ COVID-19 dataset

**Our complete COVID-19 dataset is available in [CSV](https://covid.ourworldindata.org/data/owid-covid-data.csv), [XLSX](https://covid.ourworldindata.org/data/owid-covid-data.xlsx), and [JSON](https://covid.ourworldindata.org/data/owid-covid-data.json) formats, and includes all of our historical data on the pandemic up to the date of publication.**

The CSV and XLSX files follow a format of 1 row per location and date. The JSON version is split by country ISO code, with static variables and an array of daily records.

The variables represent all of our main data related to confirmed cases, deaths, hospitalizations, and testing, as well as other variables of potential interest.

### Confirmed cases
| Variable                         | Description                                                           |
|:---------------------------------|:----------------------------------------------------------------------|
| `total_cases`                    | Total confirmed cases of COVID-19                                     |
| `new_cases`                      | New confirmed cases of COVID-19                                       |
| `new_cases_smoothed`             | New confirmed cases of COVID-19 (7-day smoothed)                      |
| `total_cases_per_million`        | Total confirmed cases of COVID-19 per 1,000,000 people                |
| `new_cases_per_million`          | New confirmed cases of COVID-19 per 1,000,000 people                  |
| `new_cases_smoothed_per_million` | New confirmed cases of COVID-19 (7-day smoothed) per 1,000,000 people |
### Confirmed deaths
| Variable                          | Description                                                             |
|:----------------------------------|:------------------------------------------------------------------------|
| `total_deaths`                    | Total deaths attributed to COVID-19                                     |
| `new_deaths`                      | New deaths attributed to COVID-19                                       |
| `new_deaths_smoothed`             | New deaths attributed to COVID-19 (7-day smoothed)                      |
| `total_deaths_per_million`        | Total deaths attributed to COVID-19 per 1,000,000 people                |
| `new_deaths_per_million`          | New deaths attributed to COVID-19 per 1,000,000 people                  |
| `new_deaths_smoothed_per_million` | New deaths attributed to COVID-19 (7-day smoothed) per 1,000,000 people |
### Excess mortality
| Variable                                  | Description                                                                                                                                                                                                                                                                                   |
|:------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `excess_mortality`                        | Percentage difference between the reported number of weekly or monthly deaths in 2020–2021 and the projected number of deaths for the same period based on previous years. For more information, see https://github.com/owid/covid-19-data/tree/master/public/data/excess_mortality           |
| `excess_mortality_cumulative`             | Percentage difference between the cumulative number of deaths since 1 January 2020 and the cumulative projected deaths for the same period based on previous years. For more information, see https://github.com/owid/covid-19-data/tree/master/public/data/excess_mortality                  |
| `excess_mortality_cumulative_absolute`    | Cumulative difference between the reported number of deaths since 1 January 2020 and the projected number of deaths for the same period based on previous years. For more information, see https://github.com/owid/covid-19-data/tree/master/public/data/excess_mortality                     |
| `excess_mortality_cumulative_per_million` | Cumulative difference between the reported number of deaths since 1 January 2020 and the projected number of deaths for the same period based on previous years, per million people. For more information, see https://github.com/owid/covid-19-data/tree/master/public/data/excess_mortality |
### Hospital & ICU
| Variable                             | Description                                                                                                    |
|:-------------------------------------|:---------------------------------------------------------------------------------------------------------------|
| `icu_patients`                       | Number of COVID-19 patients in intensive care units (ICUs) on a given day                                      |
| `icu_patients_per_million`           | Number of COVID-19 patients in intensive care units (ICUs) on a given day per 1,000,000 people                 |
| `hosp_patients`                      | Number of COVID-19 patients in hospital on a given day                                                         |
| `hosp_patients_per_million`          | Number of COVID-19 patients in hospital on a given day per 1,000,000 people                                    |
| `weekly_icu_admissions`              | Number of COVID-19 patients newly admitted to intensive care units (ICUs) in a given week                      |
| `weekly_icu_admissions_per_million`  | Number of COVID-19 patients newly admitted to intensive care units (ICUs) in a given week per 1,000,000 people |
| `weekly_hosp_admissions`             | Number of COVID-19 patients newly admitted to hospitals in a given week                                        |
| `weekly_hosp_admissions_per_million` | Number of COVID-19 patients newly admitted to hospitals in a given week per 1,000,000 people                   |
### Policy responses
| Variable           | Description                                                                                                                                                                                                         |
|:-------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `stringency_index` | Government Response Stringency Index: composite measure based on 9 response indicators including school closures, workplace closures, and travel bans, rescaled to a value from 0 to 100 (100 = strictest response) |
### Reproduction rate
| Variable            | Description                                                                                                                                   |
|:--------------------|:----------------------------------------------------------------------------------------------------------------------------------------------|
| `reproduction_rate` | Real-time estimate of the effective reproduction rate (R) of COVID-19. See https://github.com/crondonm/TrackingR/tree/main/Estimates-Database |
### Tests & positivity
| Variable                          | Description                                                                                                                                                                                                                                                                                                          |
|:----------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `total_tests`                     | Total tests for COVID-19                                                                                                                                                                                                                                                                                             |
| `new_tests`                       | New tests for COVID-19 (only calculated for consecutive days)                                                                                                                                                                                                                                                        |
| `total_tests_per_thousand`        | Total tests for COVID-19 per 1,000 people                                                                                                                                                                                                                                                                            |
| `new_tests_per_thousand`          | New tests for COVID-19 per 1,000 people                                                                                                                                                                                                                                                                              |
| `new_tests_smoothed`              | New tests for COVID-19 (7-day smoothed). For countries that don't report testing data on a daily basis, we assume that testing changed equally on a daily basis over any periods in which no data was reported. This produces a complete series of daily figures, which is then averaged over a rolling 7-day window |
| `new_tests_smoothed_per_thousand` | New tests for COVID-19 (7-day smoothed) per 1,000 people                                                                                                                                                                                                                                                             |
| `positive_rate`                   | The share of COVID-19 tests that are positive, given as a rolling 7-day average (this is the inverse of tests_per_case)                                                                                                                                                                                              |
| `tests_per_case`                  | Tests conducted per new confirmed case of COVID-19, given as a rolling 7-day average (this is the inverse of positive_rate)                                                                                                                                                                                          |
| `tests_units`                     | Units used by the location to report its testing data                                                                                                                                                                                                                                                                |
### Vaccinations
| Variable                                | Description                                                                                                                                                                                                                                                                                                                                       |
|:----------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `total_vaccinations`                    | Total number of COVID-19 vaccination doses administered                                                                                                                                                                                                                                                                                           |
| `people_vaccinated`                     | Total number of people who received at least one vaccine dose                                                                                                                                                                                                                                                                                     |
| `people_fully_vaccinated`               | Total number of people who received all doses prescribed by the vaccination protocol                                                                                                                                                                                                                                                              |
| `total_boosters`                        | Total number of COVID-19 vaccination booster doses administered (doses administered beyond the number prescribed by the vaccination protocol)                                                                                                                                                                                                     |
| `new_vaccinations`                      | New COVID-19 vaccination doses administered (only calculated for consecutive days)                                                                                                                                                                                                                                                                |
| `new_vaccinations_smoothed`             | New COVID-19 vaccination doses administered (7-day smoothed). For countries that don't report vaccination data on a daily basis, we assume that vaccination changed equally on a daily basis over any periods in which no data was reported. This produces a complete series of daily figures, which is then averaged over a rolling 7-day window |
| `total_vaccinations_per_hundred`        | Total number of COVID-19 vaccination doses administered per 100 people in the total population                                                                                                                                                                                                                                                    |
| `people_vaccinated_per_hundred`         | Total number of people who received at least one vaccine dose per 100 people in the total population                                                                                                                                                                                                                                              |
| `people_fully_vaccinated_per_hundred`   | Total number of people who received all doses prescribed by the vaccination protocol per 100 people in the total population                                                                                                                                                                                                                       |
| `total_boosters_per_hundred`            | Total number of COVID-19 vaccination booster doses administered per 100 people in the total population                                                                                                                                                                                                                                            |
| `new_vaccinations_smoothed_per_million` | New COVID-19 vaccination doses administered (7-day smoothed) per 1,000,000 people in the total population                                                                                                                                                                                                                                         |
### Others
| Variable                     | Description                                                                                                                                                                                                                                |
|:-----------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `iso_code`                   | ISO 3166-1 alpha-3 – three-letter country codes                                                                                                                                                                                            |
| `continent`                  | Continent of the geographical location                                                                                                                                                                                                     |
| `location`                   | Geographical location                                                                                                                                                                                                                      |
| `date`                       | Date of observation                                                                                                                                                                                                                        |
| `population`                 | Population in 2020                                                                                                                                                                                                                         |
| `population_density`         | Number of people divided by land area, measured in square kilometers, most recent year available                                                                                                                                           |
| `median_age`                 | Median age of the population, UN projection for 2020                                                                                                                                                                                       |
| `aged_65_older`              | Share of the population that is 65 years and older, most recent year available                                                                                                                                                             |
| `aged_70_older`              | Share of the population that is 70 years and older in 2015                                                                                                                                                                                 |
| `gdp_per_capita`             | Gross domestic product at purchasing power parity (constant 2011 international dollars), most recent year available                                                                                                                        |
| `extreme_poverty`            | Share of the population living in extreme poverty, most recent year available since 2010                                                                                                                                                   |
| `cardiovasc_death_rate`      | Death rate from cardiovascular disease in 2017 (annual number of deaths per 100,000 people)                                                                                                                                                |
| `diabetes_prevalence`        | Diabetes prevalence (% of population aged 20 to 79) in 2017                                                                                                                                                                                |
| `female_smokers`             | Share of women who smoke, most recent year available                                                                                                                                                                                       |
| `male_smokers`               | Share of men who smoke, most recent year available                                                                                                                                                                                         |
| `handwashing_facilities`     | Share of the population with basic handwashing facilities on premises, most recent year available                                                                                                                                          |
| `hospital_beds_per_thousand` | Hospital beds per 1,000 people, most recent year available since 2010                                                                                                                                                                      |
| `life_expectancy`            | Life expectancy at birth in 2019                                                                                                                                                                                                           |
| `human_development_index`    | A composite index measuring average achievement in three basic dimensions of human development—a long and healthy life, knowledge and a decent standard of living. Values for 2019, imported from http://hdr.undp.org/en/indicators/137506 |

A [full codebook](https://github.com/owid/covid-19-data/tree/master/public/data/owid-covid-codebook.csv) is made available, with a description and source for each variable in the dataset.


## Additional files and information

If you are interested in the individual files that make up the complete dataset, or more detailed information, other files can be found in the subfolders:

- [`latest`](https://github.com/owid/covid-19-data/tree/master/public/data/latest): shortened version of our complete dataset with only the latest value for each location and metric (within a limit of 2 weeks in the past). This file is available in CSV, XLSX, and JSON formats.
- [`jhu`](https://github.com/owid/covid-19-data/tree/master/public/data/jhu): data from the COVID-19 Data Repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University, related to confirmed cases and deaths. We also automatically export JHU's subnational case and death data for a few countries (Australia, Canada, China, Denmark, France, Netherlands, New Zealand, United Kingdom, United States) to a reshaped and compressed file ([`subnational_cases_deaths.zip`](https://github.com/owid/covid-19-data/blob/master/public/data/jhu/subnational_cases_deaths.zip)).
- [`testing`](https://github.com/owid/covid-19-data/tree/master/public/data/testing): data from various official sources, related to COVID-19 tests performed in each country. This folder contains two files with more detailed information:
  - [`covid-testing-all-observations.csv`](https://github.com/owid/covid-19-data/blob/master/public/data/testing/covid-testing-all-observations.csv) includes, for each historical observation, the source of the individual data point, and sometimes notes on data collection;
  - [`covid-testing-latest-data-source-details.csv`](https://github.com/owid/covid-19-data/blob/master/public/data/testing/covid-testing-latest-data-source-details.csv) includes, for each country in our testing dataset, the latest figures and a detailed description of how the country’s data is collected;
- [`excess_mortality`](https://github.com/owid/covid-19-data/tree/master/public/data/excess_mortality): data on excess mortality during the pandemic, sourced from [the Human Mortality Database](https://www.mortality.org/) and [the UK Office for National Statistics](https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/deaths/articles/comparisonsofallcausemortalitybetweeneuropeancountriesandregions/januarytojune2020);
- [`vaccinations`](https://github.com/owid/covid-19-data/tree/master/public/data/vaccinations): data from various official sources, related to COVID-19 vaccinations in each country;
- [`variants`](https://github.com/owid/covid-19-data/tree/master/public/data/variants): data related to COVID-19
  sequenced samples, classified into different variant groups;
- [`who`](https://github.com/owid/covid-19-data/tree/master/public/data/who): data from the World Health Organization, related to confirmed cases and deaths—_we have stopped using and updating this data since 18 March 2020_;
- [`ecdc`](https://github.com/owid/covid-19-data/tree/master/public/data/ecdc): data from the European Centre for Disease Prevention and Control, related to confirmed cases and deaths—_we have stopped using and updating this data since 30 November 2020_.
- [`internal`](https://github.com/owid/covid-19-data/tree/master/public/data/internal): data extracts intended for internal use at _Our World in Data_. They may change or be deleted without notice so we discourage using them.


## Changelog

- Up until 17 March 2020, we were using WHO data manually extracted from their daily [situation report PDFs](https://www.who.int/emergencies/diseases/novel-coronavirus-2019/situation-reports).
- From 19 March 2020, we started relying on data published by the [European CDC](https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide). We wrote about [why we decided to switch sources](https://ourworldindata.org/covid-sources-comparison).
- On 3 April 2020, we added country-level time series on COVID-19 tests.
- On 16 April 2020, we made available a [complete dataset of all of our main variables](https://github.com/owid/covid-19-data/tree/master/public/data) related to confirmed cases, deaths, and tests.
- On 25 April 2020, we added rows for "World" and "International" to our complete dataset. The `iso_code` column for "International" is blank, and for "World" we use `OWID_WRL`.
- On 9 May 2020, we added new variables related to demographic, economic, and public health data to our complete dataset.
- On 19 May 2020, we added 2 variables related to testing: `new_tests_smoothed` and `new_tests_smoothed_per_thousand`. To generate them we assume that testing changed equally on a daily basis over any periods in which no data was reported (as not all countries report testing data on a daily basis). This produces a complete series of daily figures, which is then averaged over a rolling 7-day window.
- On 23 May 2020, we added a JSON version of our complete dataset.
- On 4 June 2020, we added a `continent` column to our complete dataset.
- On 1 July 2020, we changed the format of the JSON version of our complete dataset to normalize the data and reduce file size.
- On 4 August 2020, we added the `positive_rate` and `tests_per_case` columns to our complete dataset.
- On 7 August 2020, we transformed our markdown codebook to a CSV file to allow easier merging with the complete dataset.
- On 17 August 2020, we added 4 variables related to cases and deaths: `new_cases_smoothed`, `new_deaths_smoothed`, `new_cases_smoothed_per_million`, and `new_deaths_smoothed_per_million`. These metrics are averaged versions (over a rolling 7-day window) of the daily variables.
- On 10 September 2020, we added the `human_development_index` column to our complete dataset.
- On 14 October 2020, we added data on excess mortality during the pandemic in the `excess_mortality` folder, sourced from [the Human Mortality Database](https://www.mortality.org/) and [the UK Office for National Statistics](https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/deaths/articles/comparisonsofallcausemortalitybetweeneuropeancountriesandregions/januarytojune2020).
- On 29 October 2020, we added data on hospitalizations and intensive care unit (ICU) admissions, sourced from the [European Centre for Disease Prevention and Control](https://www.ecdc.europa.eu/en/publications-data/download-data-hospital-and-icu-admission-rates-and-current-occupancy-covid-19) (ECDC), who provide these statistics only for a select number of European countries, and update it on a weekly basis.
- On 10 November 2020, we added data on hospitalizations and intensive care unit (ICU) admissions for the United States, sourced from the [COVID Tracking Project](https://covidtracking.com/).
- On 13 November 2020, we added real-time estimates of the effective reproduction rate (R) of the virus, sourced from [Arroyo Marioli et al. (2020)](https://doi.org/10.2139/ssrn.3581633).
- On 30 November 2020, we changed our source for confirmed cases and deaths to the [COVID-19 Data Repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University](https://github.com/CSSEGISandData/COVID-19). Our previous source for confirmed cases and deaths, the European Centre for Disease Prevention and Control (ECDC), [had announced in November 2020](https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide) that it would switch from a daily to a weekly reporting schedule from December. _Our World in Data_ therefore had to transition away from the ECDC as a source to continue to provide daily updates of confirmed cases and deaths. The data last sourced from the ECDC remains available as an archive in the [`ecdc`](https://github.com/owid/covid-19-data/tree/master/public/data/ecdc) folder. The format (variable names and types) of our complete COVID-19 dataset remains the same.
- On 9 December 2020, we changed the names of three countries in our files to match their recently-changed official names. `Czech Republic` has become `Czechia`, `Macedonia` has become `North Macedonia`, and `Swaziland` has become `Eswatini`.
- On 16 December 2020, we started collecting country-level time series on COVID-19 vaccinations.
- On 18 December 2020, we added in the [`latest`](https://github.com/owid/covid-19-data/tree/master/public/data/latest) folder a shortened version of our complete dataset with only the latest value for each location and metric (within a limit of 2 weeks in the past). This file is available in CSV, XLSX, and JSON formats.
- On 19 December 2020, we added data on hospitalizations and intensive care unit (ICU) admissions for Canada, sourced from the [COVID-19 Tracker](https://covid19tracker.ca/).
- On 6 January 2021, we added two variables for daily vaccinations to our complete dataset.
- On 7 January 2021, we replaced the United Kingdom's hospital and ICU data previously gathered by the European CDC with [the official data published by the British government](https://coronavirus.data.gov.uk/details/healthcare).
- On 26 January 2021, we added 4 variables on people vaccinated and people fully vaccinated to our complete dataset.
- On 4 February 2021, we added rows for Africa, Asia, Europe, European Union, North America, Oceania, and South America to our complete dataset. The `iso_code` column for these rows starts with `OWID_`.
- On 5 March 2021, due to [the COVID Tracking Project's announcement](https://covidtracking.com/analysis-updates/covid-tracking-project-end-march-7) that their data collection effort would stop in March 2021, we transitioned to the [Department of Health & Human Services](https://healthdata.gov/Hospital/COVID-19-Reported-Patient-Impact-and-Hospital-Capa/g62h-syeh) as our source for data on hospitalizations and ICU admissions in the United States.
- On 1 July 2021, we added a [dataset on SARS-CoV-2 variants](variants/covid-variants.csv) based on data made available by [CoVariants.org](https://covariants.org/).
- On 15 July 2021, we added data on intensive care unit (ICU) patients for Algeria, sourced from the [Ministry of Health](https://github.com/yasserkaddour/covid19-icu-data-algeria).
- On 11 August 2021, we added the metric `total_boosters` to our vaccination data. This counts the total number of booster doses (doses administered beyond the number prescribed by the vaccination protocol).
- On 12 August 2021, we added hospital and ICU data for Switzerland, sourced from the [Federal Office of Public Health](https://opendata.swiss/fr/dataset/covid-19-schweiz).
- On 28 September 2021, we changed the way we estimate the excess mortality. More details [here](https://github.com/owid/covid-19-data/tree/master/public/data/excess_mortality#how-p-scores-are-defined-and-calculated). We also added 3 new variables to our complete dataset: `excess_mortality_cumulative` (cumulative % of excess deaths), `excess_mortality_cumulative_absolute` (cumulative count of absolute excess deaths), `excess_mortality_cumulative_per_million` (cumulative count of excess deaths per million people).

## Data alterations

- The population estimates we use to calculate per-capita metrics are all based on the last revision of the [United Nations World Population Prospects](https://population.un.org/wpp/). The exact values can be viewed [here](https://github.com/owid/covid-19-data/blob/master/scripts/input/un/population_2020.csv).
- We standardize names of countries and regions. Since the names of countries and regions are different in different data sources, we standardize all names to the [_Our World in Data_ standard entity names](https://github.com/owid/covid-19-data/blob/master/public/data/jhu/locations.csv).
- We may correct or discard inconsistencies that we detect in the original data.
- Testing data is collected from many different sources. A detailed documentation for each country is available in [our post on COVID-19 testing](https://ourworldindata.org/coronavirus-testing#source-information-country-by-country).
- Where we collect multiple time series for a given country in our testing data (for example: for the United States, we collect data from both the CDC, and the COVID Tracking Project), our complete COVID-19 dataset only includes the most complete, or, if equally complete, data on the number of people tested rather than the number of tests/samples/swabs processed. The list of 'secondary' test series (those removed) is located in [`scripts/input/owid/secondary_testing_series.csv`](https://github.com/owid/covid-19-data/blob/master/scripts/input/owid/secondary_testing_series.csv).


## Stable URLs

The `/public` path of this repository is hosted at `https://covid.ourworldindata.org/`. For example, you can access the CSV for the complete dataset at `https://covid.ourworldindata.org/data/owid-covid-data.csv`.

We have the goal to keep all stable URLs working, even when we have to restructure this repository. If you need regular updates, please consider using the `covid.ourworldindata.org` URLs rather than pointing to GitHub.


## License

All visualizations, data, and code produced by _Our World in Data_ are completely open access under the [Creative Commons BY license](https://creativecommons.org/licenses/by/4.0/). You have the permission to use, distribute, and reproduce these in any medium, provided the source and authors are credited.

In the case of our vaccination dataset, please give the following citation:
> Mathieu, E., Ritchie, H., Ortiz-Ospina, E. _et al._ A global database of COVID-19 vaccinations. _Nat Hum Behav_ (2021). [https://doi.org/10.1038/s41562-021-01122-8](https://doi.org/10.1038/s41562-021-01122-8)

In the case of our testing dataset, please give the following citation:
> Hasell, J., Mathieu, E., Beltekian, D. _et al._ A cross-country database of COVID-19 testing. _Sci Data_ **7**, 345 (2020). [https://doi.org/10.1038/s41597-020-00688-8](https://doi.org/10.1038/s41597-020-00688-8)

The data produced by third parties and made available by _Our World in Data_ is subject to the license terms from the original third-party authors. We will always indicate the original source of the data in our database, and you should always check the license of any such third-party data before use.


## Authors

This data has been collected, aggregated, and documented by Cameron Appel, Diana Beltekian, Daniel Gavrilov, Charlie Giattino, Joe Hasell, Bobbie Macdonald, Edouard Mathieu, Esteban Ortiz-Ospina, Hannah Ritchie, Lucas Rodés-Guirao, Max Roser.

The mission of _Our World in Data_ is to make data and research on the world's largest problems understandable and accessible. [Read more about our mission](https://ourworldindata.org/about).
