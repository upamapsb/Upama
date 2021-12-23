import os
import importlib

import pandas as pd
from cowidev.hosp.sources import __all__ as sources
from cowidev.utils import paths


sources = [f"cowidev.hosp.sources.{s}" for s in sources]
POPULATION_FILE = os.path.join(paths.SCRIPTS.INPUT_UN, "population_latest.csv")


class HospETL:
    def extract(self):
        dfs = []
        for source in sources:
            module = importlib.import_module(source)
            df = module.main()
            assert df.indicator.isin(
                {
                    "Daily hospital occupancy",
                    "Daily ICU occupancy",
                    "Weekly new hospital admissions",
                    "Weekly new ICU admissions",
                }
            ).all()
            dfs.append(df)
        df = pd.concat(dfs)
        df = df.dropna(subset=["value"])
        assert all(
            df.groupby(["entity", "date", "indicator"]).size().reset_index()[0] == 1
        ), "Some entity-date-indicator combinations are present more than once!"
        return df

    def pipe_undo_100k(df):
        population = pd.read_csv(POPULATION_FILE, usecols=["entity", "iso_code", "population"])
        df = pd.merge(df, population, on="entity", how="left")
        assert df[df.population.isna()].shape[0] == 0, "Country missing from population file"
        df.loc[df["indicator"].str.contains(" per 100k"), "value"] = df["value"].div(100000).mul(df["population"])
        df.loc[:, "indicator"] = df["indicator"].str.replace(" per 100k", "")
        return df

    def pipe_metadata(self, df):
        print("Adding ISO & population…")
        shape_og = df.shape
        population = pd.read_csv(POPULATION_FILE, usecols=["entity", "iso_code", "population"])
        df = df.merge(population, on="entity")
        if shape_og[0] != df.shape[0]:
            raise ValueError(f"Dimension 0 after merge is different: {shape_og[0]} --> {df.shape[0]}")
        return df

    def pipe_per_million(self, df):
        print("Adding per-capita metrics…")
        per_million = df.copy()
        per_million.loc[:, "value"] = per_million["value"].div(per_million["population"]).mul(1000000)
        per_million.loc[:, "indicator"] = per_million["indicator"] + " per million"
        df = pd.concat([df, per_million], ignore_index=True).drop(columns="population")
        return df

    def pipe_round_values(self, df):
        df.loc[-df.indicator.str.contains("per million"), "value"] = df.value.round()
        return df

    def transform(self, df: pd.DataFrame):
        return (
            df.pipe(self.pipe_metadata)
            .pipe(self.pipe_per_million)
            .pipe(self.pipe_round_values)[["entity", "iso_code", "date", "indicator", "value"]]
            .sort_values(["entity", "date", "indicator"])
        )

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
