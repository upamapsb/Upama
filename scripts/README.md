# Development
[![Data](https://img.shields.io/badge/go_to-public_data-purple)](../../../public/data/)
[![Vaccinations docs](https://img.shields.io/badge/vaccination-docs-0055ff)](docs/vaccinations/README.md)
[![Testing docs](https://img.shields.io/badge/testing-docs-0055ff)](scripts/testing/README.md)

Here you will find all the different scripts and tools that we use to generate [the
data](https://github.com/owid/covid-19-data/tree/master/public/data). 

Currently, most of the  pipelines have been integrated into our [`cowidev`](src/cowidev) library.


## Contents

- [Folders](#folders)
- [Installation](#installation)
- [Getting Started](#getting-started)
- [Test data](#test-data)
- [Contribute](#contribute)

## Folders
|Folder|Description                  |
|------|-----------------------------|
|[`docs/`](docs)|Development documentation (including installation & usage of `cowidev`).|
|[`grapher/`](grapher)|Internal OWID files to power our [_grapher_](https://ourworldindata.org/owid-grapher) visualizations.|
|[`input/`](input)|External files used to compute derived metrics, such as X-per capita, and aggregate groups, such as 'Asia', etc.|
|[`output/`](output)|Temporary files. Only for development purposes. Use it at your own risk.|
|[`src/cowidev/`](src/cowidev)|`cowidev` library. It contains the code for almost all project's pipelines.|
|[`scripts`](scripts)|Legacy folder. Contains some parts of the code, such as the COVID-19 testing collection scripts. The code is a mixture of R and Python scripts.|
|[`config.yaml`](config.yaml)|Data pipeline configuration file. The default values should be working. Currently only contains configuration for vaccination data pipeline.|

Our data pipeline exports its outputs to [public/data](../public/data).


## Installation
To run or test our data pipeline, you need to install our development library `cowidev`.

### Python version
Make sure you have a working environment with Python 3 installed. We use Python >= 3.7.

You can check this with:

```
python --version
```

### Install library installation
In your environment (shell), cd to the project directory and install the library in development mode. That is, run:

```
$ pip install -e .
```

In addition to `cowidev` package, this will install the command tool `cowid-vax`, which is required
to run the vaccination data pipeline.

### Required configuration

#### Environment variables
- `{OWID_COVID_PROJECT_DIR}`: Path to the local project directory. E.g. `/Users/username/projects/covid-19-data`.
- `{OWID_COVID_VAX_CREDENTIALS_FILE}` (vaccinations): Path to the credentials file (this is internal). See next section
  for a more details explanation.
- `{OWID_COVID_VAX_CONFIG_FILE}` (vaccinations): Path to `config.yaml` file required for vaccination pipeline. By
  default, we provide this file at [scripts/config.yaml](../../scripts/config.yaml)

#### Credentials file
The environment variable `${OWID_COVID_VAX_CREDENTIALS_FILE}` references to the credentials file. It must have the following structure:

```json
{
    "greece_api_token": "[GREECE_API_TOKEN]",
    "owid_cloud_table_post": "[OWID_CLOUD_TABLE_POST]",
    "google_credentials": "[CREDENTIALS_JSON_PATH]",
    "google_spreadsheet_vax_id": "[SHEET_ID]",
    "twitter_consumer_key": "[TWITTER_CONSUMER_KEY]",
    "twitter_consumer_secret": "[TWITTER_CONSUMER_SECRET]"
}
```

Most of the fields are self-explanatory, but if you need help setting them up please contact us or create an issue. Note
that Google-related fields require a valid OAuth JSON credentials file (see [gsheets documentation](https://gsheets.readthedocs.io/en/stable/#quickstart)).

**IMPORTANT**: This is an internal file, so we won't be sharing it. This file is needed to run the complete pipeline, but is
not necessary if you just want to run the scraping process for a country (e.g. for vaccinations `$ cowid-vax get -c
kenya`).


## Getting started
The data pipeline is built from different processes, which are executed separately. To run a process use:

```
python -m cowidev.[process] [step]
```

where the `[process]` can be:

- `gmoblity`: Google mobility.
- `hosp`: Hospitalizations.
- `oxcgrt`: COVID-19 government response tracker.
- `variants`: COVID-19 variants.
- `xm`: Excess mortality.
- `yougov`: COVID-19 behaviour tracker.


and `[step]` can be:

- `etl`: Get the data into a data-science ready file (DS-ready file).
- `grapher-file`: Using the DS-ready file, generate a grapher-ready file.
- `explorer-file`: Using the DS-ready file, generate an explorer-ready file.
- `grapher-db`: Update the Grapher DB using the grapher-ready file.


**NOTE: The `vaccinations` process does not follow this structure. For more info, check the [vaccination process
overview](scripts/docs/vaccinations/README.md) and the [vaccination data contribution guideline](scripts/docs/vaccinations/CONTRIBUTE.md). Also, testing is not currently available within in the library.**

## Test data
Currently, the testing data is not included in the library, but in the folder [`scripts/testing/`](scripts/testing).

It resembles very much the architecture of the vaccination pipeline, but differs in some key points. The most noticeable
difference is that it contains both R and Python code. We currently prefer contributions in Python.


_More info:_
   - [Testing data overview](scripts/testing/README.md)
   - [Testing data contribution guideline](scripts/testing/CONTRIBUTE.md)

## Contribute
We welcome contributions for all of our processes. There are two types of contributions:

- **Maintenance/Enhancements**: Improve processes currently available in the library. E.g. add a new country scrapper for
  the vaccinations data. There are no generic guidelines for our contributions except for
  [vaccination](docs/vaccinations/CONTRIBUTE.md) and [testing](scripts/testing/CONTRIBUTE.md) processes.
- **New features**: Create a new process in the library. An ongoing example for this is the [testing](scripts/testing),
  for which we are moving its logic to the library. For more details on this, check discussion #2099.
