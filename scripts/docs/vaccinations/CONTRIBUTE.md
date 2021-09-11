# Contribute - Vaccination data
**THIS FILE IS BEING RE-WRITTEN**


We welcome contributions to our vaccination dataset! Note that due to the nature of our pipeline, **we cannot accept pull requests
for manually imported country data**. To see which countries are automated and which reaquire manual import, check
[this file](../../output/vaccinations/automation_state.csv).

### Content
- [About our vaccination data](#about-our-vaccination-data)
  - [Manufacturer data](#Manufacturer-data)
  - [Age group data](#Age-group-data)
- [Report new data values](#report-new-data-values)
- [Add new country automations](#Add-new-country-automations)
  - [Steps](#Steps)
- [Criteria to accept pull requests](#criteria-to-accept-pull-requests)

## About our vaccination data
For details about the development environment, check the details in [README](README.md#2-development-environment).


We are currently collecting vaccination data at country level in the following format:
|location|date      |vaccine                                                        |source_url                                                                                   |total_vaccinations|people_vaccinated|people_fully_vaccinated|total_boosters|
|--------|----------|---------------------------------------------------------------|---------------------------------------------------------------------------------------------|------------------|-----------------|-----------------------|--------------|
|Cambodia|2021-09-10|Johnson&Johnson, Oxford/AstraZeneca, Sinopharm/Beijing, Sinovac|https://www.facebook.com/MinistryofHealthofCambodia/photos/a.930887636950343/4376835072355565|20554497          |11406989         |9350408                |742293        |


Where metrics:

- `total_vaccinations`
- `people_vaccinated`
- `people_fully_vaccinated`
- `total_boosters`

are defined [here](https://github.com/owid/covid-19-data/tree/master/public/data#vaccinations).
 
Note that for some countries, some metrics can't be reported as these are not be available. This is not ideal but it is OK.

### Manufacturer data
Along with the main data, we include vaccine data break by manufacturer for some countries where this data is available.

Each row in the data gives the cumulative number of doses administered for a given date and vaccine manufacturer.

#### Fields
- `date`: Date in format YYYY-MM-DD
- `vaccine`: Vaccine manufacturer name. Our convention for vaccine names can be found
  [here](https://github.com/owid/covid-19-data/blob/c4208f353449d750515b8e14015bde2c349371ee/scripts/scripts/vaccinations/src/vax/utils/checks.py#L7).
  As new vaccines emerge, new conventions will be defined.
- `location`: Region name.
- `total_vaccinations`: Cumulative number of administered doses up to `date` for given `vaccine`.


#### Example
|date      |vaccine           |location|total_vaccinations |
|----------|------------------|------------------|---------|
|...|...           |...            |...|
|2021-06-01|Moderna           |Lithuania            |151261|
|2021-06-01|Oxford/AstraZeneca|Lithuania            |333733|
|2021-06-01|Johnson&Johnson   |Lithuania             |34974|
|2021-06-01|Pfizer/BioNTech   |Lithuania           |1133371|
|...|...           |...            |...|

#### Notes
We only include manufacturer data for countries for which the process can be automated. No manual reports are currently
being accepted. This is to ensure scalability of the project.


### Age group data

Along with the main data, we include vaccine data break by age groups for some countries where the data is available.

Each row in the data gives the percentage of people within an age group that have received at least one dose. Note that
currently there is no standard for which age groups are accepted, as each country may define different ones. As a
general rule, we try to have groups in 10 years chunks.

**Note that the reported metric is relative, and not absolute.**
#### Fields
- `date`: Date in format YYYY-MM-DD
- `age_group_min`: Lower bound of the age group.
- `age_group_max`: Upper bound of the age group (included).
- `location`: Region name.
- `people_vaccinated_per_hundred`: Percentage of people within the age group that have received at least one dose.
- `people_fully_vaccinated_per_hundred`: Percentage of people within the age group that have been fully vaccinated.

#### Example
|location | date |age_group_min |age_group_max|people_vaccinated_per_hundred|people_fully_vaccinated_per_hundred|
|----------|------------------|-------------|------------------|--------|--------|
|...|...           |...            |...|...|...|
Poland|2021-06-08|18|24|26.77|7.33|
Poland|2021-06-08|25|49|36.01|14.2|
Poland|2021-06-08|50|59|50.68|30.22|
Poland|2021-06-08|60|69|63.05|35.67|
Poland|2021-06-08|70|79|77.45|70.7|
Poland|2021-06-08|80||59.94|56.55|
|...|...           |...            |...|...|...|

#### Notes
We only include age group data for countries for which the process can be automated. No manual reports are currently
being accepted. This is to ensure scalability of the project.

## Report new data values
To report new values for a country/location, first check if the imports for that country/territory are automated. You
can check column `automated` in [this file](../../output/vaccinations/automation_state.csv).

- If the country imports are automated (`TRUE` value in file above), note that the new value might be added in next
  update. **Only report new values if the data is missing for more than 48 hours!** Report the new data as a [pull request](https://github.com/owid/covid-19-data/compare).
- If the country imports are not automated, i.e. data is manually added, (`FALSE` value in file above) you can report
  new data in any of the following ways:
  - Open a [new issue](https://github.com/owid/covid-19-data/issues/new), reporting the data and the corresponding
    source.
  - If you plan to contribute regularly to a specific country/location, consider opening a dedicated issue. This way,
    we can easily back-track the data addded for that country/location.
  - If this seems too complicated, alternatively, you may simply add a comment to thread
[#230](https://github.com/owid/covid-19-data/issues/230). 

### Notes
- We only accept official sources or news correctly citing official sources.
- We only accept manual reports for country aggregate vaccination data. That is, we currently do not include
  manufacturer and age vaccination data if no automation is provided.

## Add new country automations
To automate the data import for a country, make sure that:
- The source is reliable.
- The source provides data in a format that can be easily read:
    - As a file (e.g. csv, json, xls, etc.)
    - As plain text in source HTML, which can be easily scraped.

### Steps
Next, follow the steps below:

1. Decide if the import is batch (i.e. all the timeseries) or incremental (last value). See the scripts in
   [`src/cowidev/vax/batch`](../../src/cowidev/vax/batch) and [`src/cowidev/vax/incremental`](../../src/cowidev/vax/incremental) for more details. **Note: Batch is
   prefered over Incremental**.
2. Create a script and place it based on decision in step 1 either in [`src/cowidev/vax/batch`](../../src/cowidev/vax/batch) or
   [`src/cowidev/vax/incremental`](../../src/cowidev/vax/incremental). Note that each source is different and there is no single pattern that
   works for all sources, however you can take some inspiration from the scripts below:
    - Batch imports:
        - CSV: [Peru](../../src/cowidev/vax/batch/peru.py)
        - JSON: [Hong Kong](../../src/cowidev/vax/batch/hong_kong.py)
        - API/JSON (with manufacturer data): [Lithuania](../../src/cowidev/vax/batch/lithuania.py)
        - CSV (with manufacturer data): [Romania](../../src/cowidev/vax/batch/romania.py)
        - Excel (with manufacturer data): [Latvia](../../src/cowidev/vax/batch/latvia.py)
        - Link to excel (with age data): [New Zealand](../../src/cowidev/vax/batch/new_zealand.py)
    - Incremental imports:
        - HTML: [Bulgaria](../../src/cowidev/vax/incremental/bulgaria.py), [Equatorial Guinea](../../src/cowidev/vax/incremental/equatorial_guinea.py)
        - HTML, from news feed: [Macao](../../src/cowidev/vax/incremental/macao.py), [Albania](../../src/cowidev/vax/incremental/albania.py), [Monaco](../../src/cowidev/vax/incremental/monaco.py) 
        - PDF: [Taiwan](../../src/cowidev/vax/incremental/taiwan.py), [Nepal](../../src/cowidev/vax/incremental/nepal.py)
        - CSV: [Argentina](../../src/cowidev/vax/incremental/argentina.py)
        - API/JSON: [Poland](../../src/cowidev/vax/incremental/poland.py)
    - Others:
        - From WHO: See [WHO](../../src/cowidev/vax/incremental/who.py)
        - From SPC: See [SPC](../../src/cowidev/vax/batch/spc.py)
3. Feel free to add [manufacturer](#manufacturer-data)/[age data](#age-group-data) if you are automating a batch script and
   the data is available.
4. Test that it is working and that it is stable. For this you need to have the [library
   installed](README.md#2-development-environment). Run
```
cowid-vax get -c [country-name]
``` 
   
5. Issue a pull request and wait for a review.



More details: [#230](https://github.com/owid/covid-19-data/issues/230),
[#250](https://github.com/owid/covid-19-data/issues/250)

## Criteria to accept pull requests
Due to how our pipeline operates at the moment, pull requests are only accepted under certain conditions. These include,
but are not limited to, the following:

- Code improvements / bug fixes. As an example, you can take [#465](https://github.com/owid/covid-19-data/pull/465).
- Updates on the data for countries with automated data imports and incremental processes (this countries are found
  [here](../../src/cowidev/vax/incremental)). For this case, you can create a PR modifying the corresponding file in [output
  folder](https://github.com/owid/covid-19-data/tree/master/scripts/output/vaccinations). Create the pull
  request only if the daily update already ran but did not update the corresponding country.

You can of course, and we appreciate it very much, create pull requests for other cases.

Note that files in [public folder](https://github.com/owid/covid-19-data/tree/master/public) are not to be manually modified.
