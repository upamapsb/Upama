import json

import pandas as pd

from cowidev.utils.clean.dates import localdate
from cowidev.utils.web import request_json
from cowidev.vax.utils.incremental import enrich_data, increment


class Lebanon:
    location: str = "Lebanon"
    source_url: str = "https://impactpublicdashboard.cib.gov.lb/s/public/elasticsearch/vaccine_registration_event_data/_search?rest_total_hits_as_int=true&ignore_unavailable=true&ignore_throttled=true&preference=1635837427794&timeout=30000ms"
    source_url_ref: str = "https://impact.cib.gov.lb/home/dashboard/vaccine"
    headers: dict = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:93.0) Gecko/20100101 Firefox/93.0",
        "Accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "kbn-version": "7.6.2",
        "Origin": "https://impactpublicdashboard.cib.gov.lb",
        "Connection": "keep-alive",
        "Referer": "https://dashboard.impactlebanon.com/s/public/app/kibana",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }

    @property
    def people_vaccinated_query(self):
        return """
            {"aggs":{},"size":0,"stored_fields":["*"],"script_fields":{"vaccine_registration_age":{"script":{"source":"if (doc[\'vaccine_registration_date_of_birth\'].size()==0) {\\n    return -1 \\n}\\nelse {\\n    Instant instant = Instant.ofEpochMilli(new Date().getTime());\\nZonedDateTime birth = doc[\'vaccine_registration_date_of_birth\'].value;\\nZonedDateTime now = ZonedDateTime.ofInstant(instant, ZoneId.of(\'Z\'));\\nreturn ChronoUnit.YEARS.between(birth, now)\\n}\\n","lang":"painless"}}},"docvalue_fields":[{"field":"@timestamp","format":"date_time"},{"field":"batch_creation_date_time","format":"date_time"},{"field":"batch_proposed_vaccination_date","format":"date_time"},{"field":"event_last_updated_date_time","format":"date_time"},{"field":"last_updated_date_time","format":"date_time"},{"field":"vaccine_registration_covid_infection_date","format":"date_time"},{"field":"vaccine_registration_creation_date_time","format":"date_time"},{"field":"vaccine_registration_date","format":"date_time"},{"field":"vaccine_registration_date_of_birth","format":"date_time"},{"field":"vaccine_registration_event_creation_date_time","format":"date_time"},{"field":"vaccine_registration_event_date","format":"date_time"},{"field":"vaccine_registration_event_vaccination_date","format":"date_time"},{"field":"vaccine_registration_first_dose_vaccination_date","format":"date_time"},{"field":"vaccine_registration_previous_vaccine_date","format":"date_time"},{"field":"vaccine_registration_upload_date","format":"date_time"}],"_source":{"excludes":[]},"query":{"bool":{"must":[],"filter":[{"match_all":{}},{"bool":{"filter":[{"bool":{"must_not":{"bool":{"should":[{"match":{"vaccine_registration_is_duplicate":1}}],"minimum_should_match":1}}}},{"bool":{"filter":[{"bool":{"must_not":{"bool":{"should":[{"match":{"vaccine_registration_event_logical_delete":1}}],"minimum_should_match":1}}}},{"bool":{"filter":[{"bool":{"must_not":{"bool":{"should":[{"match":{"vaccine_registration_logical_delete":1}}],"minimum_should_match":1}}}},{"bool":{"should":[{"exists":{"field":"vaccine_registration_date_of_birth"}}],"minimum_should_match":1}}]}}]}}]}},{"match_phrase":{"event_status.keyword":"DONE"}},{"match_phrase":{"vaccine_registration_event_dose_number":"1"}},{"range":{"vaccine_registration_event_creation_date_time":{"gte":"2018-06-01T04:51:47.181Z","lte":"2023-05-01T04:51:23.196Z","format":"strict_date_optional_time"}}}],"should":[],"must_not":[]}}}
        """

    @property
    def people_fully_vaccinated_query(self):
        return """
            {"aggs":{},"size":0,"stored_fields":["*"],"script_fields":{"vaccine_registration_age":{"script":{"source":"if (doc[\'vaccine_registration_date_of_birth\'].size()==0) {\\n    return -1 \\n}\\nelse {\\n    Instant instant = Instant.ofEpochMilli(new Date().getTime());\\nZonedDateTime birth = doc[\'vaccine_registration_date_of_birth\'].value;\\nZonedDateTime now = ZonedDateTime.ofInstant(instant, ZoneId.of(\'Z\'));\\nreturn ChronoUnit.YEARS.between(birth, now)\\n}\\n","lang":"painless"}}},"docvalue_fields":[{"field":"@timestamp","format":"date_time"},{"field":"batch_creation_date_time","format":"date_time"},{"field":"batch_proposed_vaccination_date","format":"date_time"},{"field":"event_last_updated_date_time","format":"date_time"},{"field":"last_updated_date_time","format":"date_time"},{"field":"vaccine_registration_covid_infection_date","format":"date_time"},{"field":"vaccine_registration_creation_date_time","format":"date_time"},{"field":"vaccine_registration_date","format":"date_time"},{"field":"vaccine_registration_date_of_birth","format":"date_time"},{"field":"vaccine_registration_event_creation_date_time","format":"date_time"},{"field":"vaccine_registration_event_date","format":"date_time"},{"field":"vaccine_registration_event_vaccination_date","format":"date_time"},{"field":"vaccine_registration_first_dose_vaccination_date","format":"date_time"},{"field":"vaccine_registration_previous_vaccine_date","format":"date_time"},{"field":"vaccine_registration_upload_date","format":"date_time"}],"_source":{"excludes":[]},"query":{"bool":{"must":[],"filter":[{"match_all":{}},{"bool":{"filter":[{"bool":{"must_not":{"bool":{"should":[{"match":{"vaccine_registration_is_duplicate":1}}],"minimum_should_match":1}}}},{"bool":{"filter":[{"bool":{"must_not":{"bool":{"should":[{"match":{"vaccine_registration_event_logical_delete":1}}],"minimum_should_match":1}}}},{"bool":{"filter":[{"bool":{"must_not":{"bool":{"should":[{"match":{"vaccine_registration_logical_delete":1}}],"minimum_should_match":1}}}},{"bool":{"should":[{"exists":{"field":"vaccine_registration_date_of_birth"}}],"minimum_should_match":1}}]}}]}}]}},{"match_phrase":{"event_status.keyword":"DONE"}},{"match_phrase":{"vaccine_registration_event_dose_number":"2"}},{"range":{"vaccine_registration_event_creation_date_time":{"gte":"2018-06-01T04:51:47.181Z","lte":"2023-05-01T04:51:23.196Z","format":"strict_date_optional_time"}}}],"should":[],"must_not":[]}}}
        """

    @property
    def total_boosters_query(self):
        return """
            {"aggs":{},"size":0,"stored_fields":["*"],"script_fields":{"vaccine_registration_age":{"script":{"source":"if (doc[\'vaccine_registration_date_of_birth\'].size()==0) {\\n    return -1 \\n}\\nelse {\\n    Instant instant = Instant.ofEpochMilli(new Date().getTime());\\nZonedDateTime birth = doc[\'vaccine_registration_date_of_birth\'].value;\\nZonedDateTime now = ZonedDateTime.ofInstant(instant, ZoneId.of(\'Z\'));\\nreturn ChronoUnit.YEARS.between(birth, now)\\n}\\n","lang":"painless"}}},"docvalue_fields":[{"field":"@timestamp","format":"date_time"},{"field":"batch_creation_date_time","format":"date_time"},{"field":"batch_proposed_vaccination_date","format":"date_time"},{"field":"event_last_updated_date_time","format":"date_time"},{"field":"last_updated_date_time","format":"date_time"},{"field":"vaccine_registration_covid_infection_date","format":"date_time"},{"field":"vaccine_registration_creation_date_time","format":"date_time"},{"field":"vaccine_registration_date","format":"date_time"},{"field":"vaccine_registration_date_of_birth","format":"date_time"},{"field":"vaccine_registration_event_creation_date_time","format":"date_time"},{"field":"vaccine_registration_event_date","format":"date_time"},{"field":"vaccine_registration_event_vaccination_date","format":"date_time"},{"field":"vaccine_registration_first_dose_vaccination_date","format":"date_time"},{"field":"vaccine_registration_previous_vaccine_date","format":"date_time"},{"field":"vaccine_registration_upload_date","format":"date_time"}],"_source":{"excludes":[]},"query":{"bool":{"must":[],"filter":[{"match_all":{}},{"bool":{"filter":[{"bool":{"must_not":{"bool":{"should":[{"match":{"vaccine_registration_is_duplicate":1}}],"minimum_should_match":1}}}},{"bool":{"filter":[{"bool":{"must_not":{"bool":{"should":[{"match":{"vaccine_registration_event_logical_delete":1}}],"minimum_should_match":1}}}},{"bool":{"filter":[{"bool":{"must_not":{"bool":{"should":[{"match":{"vaccine_registration_logical_delete":1}}],"minimum_should_match":1}}}},{"bool":{"filter":[{"bool":{"should":[{"exists":{"field":"vaccine_registration_date_of_birth"}}],"minimum_should_match":1}},{"bool":{"filter":[{"bool":{"should":[{"exists":{"field":"gender.keyword"}}],"minimum_should_match":1}},{"bool":{"should":[{"exists":{"field":"nationality.keyword"}}],"minimum_should_match":1}}]}}]}}]}}]}}]}},{"match_phrase":{"event_status.keyword":"DONE"}},{"match_phrase":{"vaccine_registration_event_dose_number":"3"}},{"range":{"vaccine_registration_event_creation_date_time":{"gte":"2018-06-01T04:51:47.181Z","lte":"2023-05-01T04:51:23.196Z","format":"strict_date_optional_time"}}}],"should":[],"must_not":[]}}}
        """

    def read(self) -> pd.Series:
        people_vaccinated = self._get_api_value(self.people_vaccinated_query)
        people_fully_vaccinated = self._get_api_value(self.people_fully_vaccinated_query)
        total_boosters = self._get_api_value(self.total_boosters_query)
        total_vaccinations = people_vaccinated + people_fully_vaccinated + total_boosters

        return pd.Series(
            {
                "total_vaccinations": total_vaccinations,
                "people_fully_vaccinated": people_fully_vaccinated,
                "people_vaccinated": people_vaccinated,
                "total_boosters": total_boosters,
            }
        )

    def _get_api_value(self, query: str):
        query = json.loads(query)
        data = request_json(self.source_url, json=query, headers=self.headers, request_method="post")
        value = int(data["hits"]["total"])
        return value

    def pipe_date(self, ds: pd.Series) -> pd.Series:
        date = localdate("Asia/Beirut")
        return enrich_data(ds, "date", date)

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds,
            "vaccine",
            "Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sputnik V",
        )

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url_ref)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_date).pipe(self.pipe_location).pipe(self.pipe_vaccine).pipe(self.pipe_source)

    def export(self):
        data = self.read().pipe(self.pipeline)
        increment(
            location=data["location"],
            total_vaccinations=data["total_vaccinations"],
            people_vaccinated=data["people_vaccinated"],
            people_fully_vaccinated=data["people_fully_vaccinated"],
            total_boosters=data["total_boosters"],
            date=data["date"],
            source_url=data["source_url"],
            vaccine=data["vaccine"],
        )


def main():
    Lebanon().export()


if __name__ == "__main__":
    main()
