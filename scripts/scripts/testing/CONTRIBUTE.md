# Contribute - Test data
We welcome contributions to our testing dataset! 

Automated countries can be found under [automations](automations) folder. Some countries have a _batch_ process while
others an _incremental_ one.

- **batch**: Complete timeseries is updated in every execution. This process is prefered, as it means the source can correct past data.
- **incremental**: Only last data point is added. 

The code consists of a mixture of python and R scripts. While contributions in both languages are more than welcome, we
prefer python.

### Content
- [Add a new country](#add-a-new-country)
- [Criteria to accept pull requests](#criteria-to-accept-pull-requests)

### Add a new country
To automate the data import for a country, make sure that:
- The source is reliable.
- The source provides data in a format that can be easily read:
    - As a file (e.g. csv, json, xls, etc.)
    - As plain text in source HTML, which can be easily scraped.

#### Steps
1. Decide if the import is batch (i.e. all the timeseries) or incremental (last value). See the scripts in
   [`automations/batch`](automations/batch) and [`automations/incremental`](automations/incremental) for more details. **Note: Batch is prefered over Incremental**.
2. Create a script and place it based on decision in step 1 either in [`automations/batch`](automations/batch) or
   [`automations/incremental`](automations/incremental). Note that each source is different and there is no single pattern that works for all sources, however you can take some inspiration from the scripts below:
    - Batch imports:
        - HTML table: [Turkey](automations/batch/turkey.py)
        - HTML elements: [Slovenia](automations/batch/turkey.py)
        - API/JSON: [Portugal](automations/batch/portugal.py)
        - CSV: [France](automations/batch/france.py)
    - Incremental imports:
        - CSV: [Equatorial Guinea](automations/incremental/equatorial-guinea.py)
        - HTML elements: [Belize](automations/incremental/belize.py)
4. Test that it is working and that it is stable.
5. Create a pull request with your code!


## Criteria to accept pull requests
- Limit your pull request to a single country or a single feature.
- We welcome code improvements / bug fixes. As an example, you can take [#465](https://github.com/owid/covid-19-data/pull/465).

You can of course, and we appreciate it very much, create pull requests for other cases.

Note that files in [public folder](https://github.com/owid/covid-19-data/tree/master/public) are not to be modified via
Pull requests.