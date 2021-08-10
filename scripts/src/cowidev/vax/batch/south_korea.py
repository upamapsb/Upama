import pandas as pd

from cowidev.vax.utils.utils import read_xlsx_from_url, clean_df_columns_multiindex
from cowidev.vax.utils.dates import clean_date_series


class South_korea:
    def __init__(self):
        self.location = "South Korea"
        self.source_url = (
            "https://ncv.kdca.go.kr/boardDownload.es?bid=0037&list_no=443&seq=1"
        )
        self.source_url_ref = "https://ncv.kdca.go.kr/"
        self.vaccines_mapping = {
            "모더나 누적": "Moderna",
            "아스트라제네카 누적": "Oxford/AstraZeneca",
            "화이자 누적": "Pfizer/BioNTech",
            "얀센 누적": "Johnson&Johnson"
        }

    def read(self):
        df = read_xlsx_from_url(self.source_url, header=[4, 5])
        df = clean_df_columns_multiindex(df)
        return df

    def pipe_check_format(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.shape[1] != 10:
            raise ValueError("Number of columns has changed!")
        columns_lv0 = {"모더나 누적", "아스트라제네카 누적", "얀센 누적", "일자", "전체 누적", "화이자 누적"}
        columns_lv1 = {"", "1차", "1차(완료)", "완료", "완료\n(AZ-PF교차미포함)", "완료\n(AZ-PF교차포함)"}
        columns_lv0_wrong = df.columns.levels[0].difference(columns_lv0)
        columns_lv1_wrong = df.columns.levels[1].difference(columns_lv1)
        if columns_lv0_wrong.any():
            raise ValueError(f"Unknown columns in level 0: {columns_lv0_wrong}")
        if columns_lv1_wrong.any():
            raise ValueError(f"Unknown columns in level 1: {columns_lv1_wrong}")
        return df

    def pipe_extract(self, df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": df.loc[:, "일자"],
                "people_vaccinated": df.loc[:, ("전체 누적", "1차")],
                "people_fully_vaccinated": df.loc[:, ("전체 누적", "완료")],
                "janssen": df.loc[:, ("얀센 누적", "1차(완료)")],
            }
        )

    def pipe_source(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(source_url=self.source_url_ref)

    def pipe_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location)

    def pipe_total_vaccinations(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            total_vaccinations=df["people_vaccinated"]
            + df["people_fully_vaccinated"]
            - df["janssen"]
        )

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(date=clean_date_series(df.date, "%Y-%m-%d"))

    def pipe_vac(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine(date: str):
            if date >= "2021-06-18":
                return "Oxford/AstraZeneca, Pfizer/BioNTech, Johnson&Johnson, Moderna"
            elif date >= "2021-06-10":
                return "Oxford/AstraZeneca, Pfizer/BioNTech, Johnson&Johnson"
            elif date >= "2021-02-27":
                return "Oxford/AstraZeneca, Pfizer/BioNTech"
            elif date >= "2021-02-26":
                return "Oxford/AstraZeneca"

        return df.assign(vaccine=df.date.apply(_enrich_vaccine))

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_check_format)
            .pipe(self.pipe_extract)
            .pipe(self.pipe_total_vaccinations)
            .pipe(self.pipe_date)
            .pipe(self.pipe_source)
            .pipe(self.pipe_location)
            .pipe(self.pipe_vac)[
                [
                    "location",
                    "date",
                    "vaccine",
                    "source_url",
                    "total_vaccinations",
                    "people_vaccinated",
                    "people_fully_vaccinated",
                ]
            ]
            .sort_values("date")
            .drop_duplicates()
        )

    def export(self, paths):
        df = self.read()
        df.pipe(self.pipeline).to_csv(paths.tmp_vax_out(self.location), index=False)


def main(paths):
    South_korea().export(paths)
