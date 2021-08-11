## Set up Development environment
### Python version
Make sure you have a working environment with Python 3 installed. We use Python >= 3.7.

You can check this with:

```
python --version
```

### Install library
In your environment (shell), cd to the project directory and install the library in development mode. That is, run:

```
$ pip install -e .
```

In addition to `cowidev` package, this will install the command tool `cowid-vax`, which is required
to run the data pipeline.

### Required configuration

#### Environment varilables
- `{OWID_COVID_PROJECT_DIR}`: Path to the local project directory. E.g. `/Users/username/projects/covid-19-data`.
- `{OWID_COVID_VAX_CREDENTIALS_FILE}` (vaccinations): Path to the credentials file (this is internal). Google-related fields require a valid OAuth JSON credentials file (see [gsheets
  documentation](https://gsheets.readthedocs.io/en/stable/#quickstart)). The credentials file should have the following structure:
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
- `{OWID_COVID_VAX_CONFIG_FILE}` (vaccinations): Path to `config.yaml` file required for vaccination pipeline.

#### Credentials file
The environment variable `${OWID_COVID_VAX_CREDENTIALS_FILE}` corresponds to the path to the credentials file. This is internal. Google-related fields require a valid OAuth JSON credentials file (see [gsheets
  documentation](https://gsheets.readthedocs.io/en/stable/#quickstart)). The file should have the following structure:
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