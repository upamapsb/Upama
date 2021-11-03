import importlib

import pandas as pd
from cowidev.hosp.sources import __all__ as sources


sources = [f"cowidev.hosp.sources.{s}" for s in sources]


class HospETL:
    def __init__(self) -> None:
        self.source_url = "https://opendata.ecdc.europa.eu/covid19/hospitalicuadmissionrates/csv/data.csv"

    def extract(self):
        dfs = []
        for source in sources:
            module = importlib.import_module(source)
            df = module.main()
            dfs.append(df)
        df = pd.concat(dfs)
        df = df.dropna(subset=["value"])
        return df

    def pipe_per_million(self, df):
        print("Adding per-capita metrics…")
        per_million = df.copy()
        per_million.loc[:, "value"] = per_million["value"].div(per_million["population"]).mul(1000000)
        per_million.loc[:, "indicator"] = per_million["indicator"] + " per million"
        df = pd.concat([df, per_million]).drop(columns="population")
        return df

    def transform(self, df: pd.DataFrame):
        return df.pipe(self.pipe_per_million)

    def load(self, df: pd.DataFrame, output_path: str) -> None:
        # Export data
        df.to_csv(output_path, index=False)

    def run(self, output_path: str):
        df = self.extract()
        df = self.transform(df)
        self.load(df, output_path)


def run_etl(output_path: str):
    etl = HospETL()
    etl.run(output_path)
