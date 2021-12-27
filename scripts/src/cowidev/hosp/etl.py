import os
import time
import importlib
from joblib import Parallel, delayed

import pandas as pd
from pandas.api.types import is_string_dtype
from cowidev.utils import paths
from cowidev.utils.log import get_logger
from cowidev.hosp.sources import __all__ as sources


sources = [f"cowidev.hosp.sources.{s}" for s in sources]
POPULATION_FILE = os.path.join(paths.SCRIPTS.INPUT_UN, "population_latest.csv")
logger = get_logger()


class HospETL:
    def extract(
        self,
        parallel: bool = False,
        n_jobs: int = -2,
    ):
        """Get the data for all locations.

        - Build preliminary dataframe with all locations data.
        - Build metadata dataframe with locations metadata (source url, source name, etc.)
        """
        t0 = time.time()
        # Get data
        if parallel:
            modules_execution_results = Parallel(n_jobs=n_jobs, backend="threading")(
                delayed(self._extract_entity)(
                    source,
                )
                for source in sources
            )
        else:
            modules_execution_results = [self._extract_entity(source) for source in sources]
        # Build output
        df = pd.concat([m[0] for m in modules_execution_results if m is not None])
        df_meta = self._build_metadata(modules_execution_results)
        # Process output
        df = df.dropna(subset=["value"])
        assert all(
            df.groupby(["entity", "date", "indicator"]).size().reset_index()[0] == 1
        ), "Some entity-date-indicator combinations are present more than once!"
        # Execution details
        self._execution_summary(t0, modules_execution_results)
        return df, df_meta

    def _build_metadata(self, modules_execution_results):
        """Build metadata dataframe (to be exported later to locations.csv)."""
        # Filter out Nones
        metadata = [m[1] for m in modules_execution_results if m is not None]
        # Flatten list
        metadata = [[m] if not isinstance(m, list) else m for m in metadata]
        metadata = [mm for m in metadata for mm in m]
        # Build dataframe
        metadata = [
            {"location": m["entity"], "source_name": m["source_name"], "source_website": m["source_url_ref"]}
            for m in metadata
        ]
        df_meta = pd.DataFrame.from_records(metadata)
        return df_meta

    def _extract_entity(self, module_name: str):
        """Execute the process to get the data for a certain location (country)."""
        t0 = time.time()
        module = importlib.import_module(module_name)
        logger.info(f"HOSP - {module_name}: started")
        try:
            df, metadata = module.main()
        except Exception as err:
            logger.error(f"HOSP - {module_name}: ❌ {err}", exc_info=True)
            return None
        else:
            self._check_fields_df(df)
            # Execution details
            t = round(time.time() - t0, 2)
            execution = {
                "module_name": module_name,
                "time": t,
            }
            logger.info(f"HOSP - {module_name}: SUCCESS ✅")
            return df, metadata, execution

    def _check_fields_df(self, df):
        """Check format of the data collected for a certain location."""
        assert df.indicator.isin(
            {
                "Daily hospital occupancy",
                "Daily ICU occupancy",
                "Weekly new hospital admissions",
                "Weekly new ICU admissions",
            }
        ).all(), "One of the indicators for this country is not recognized!"
        assert is_string_dtype(df.date), "The date column is not a string!"

    def _build_time_df(self, execution):
        """Build execution time dataframe."""
        df_time = (
            pd.DataFrame([{"module": m["module_name"], "execution_time (sec)": m["time"]} for m in execution])
            .set_index("module")
            .sort_values(by="execution_time (sec)", ascending=False)
        )
        return df_time

    def _execution_summary(self, t0, modules_execution_results):
        """Print a summary from the execution (timings)."""
        execution = [m[2] for m in modules_execution_results if m is not None]
        df_time = self._build_time_df(execution)
        t_sec_1 = round(time.time() - t0, 2)
        t_min_1 = round(t_sec_1 / 60, 2)
        print("---")
        print("TIMING DETAILS")
        print(f"Took {t_sec_1} seconds (i.e. {t_min_1} minutes).")
        print(f"Top most time consuming scripts:")
        print(df_time.head(20))

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
        per_million.loc[:, "value"] = per_million["value"].div(per_million["population"]).mul(1000000).round(3)
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

    def transform_meta(self, df_meta: pd.DataFrame, df: pd.DataFrame):
        df_ = (
            df.groupby(["entity", "iso_code"], as_index=False)
            .date.max()
            .rename(columns={"date": "last_observation_date"})
        )
        df_meta = df_meta.merge(df_, left_on="location", right_on="entity", how="left")
        df_meta = df_meta[["location", "iso_code", "last_observation_date", "source_name", "source_website"]]
        return df_meta

    def load(self, df: pd.DataFrame, output_path: str) -> None:
        # Export data
        df.to_csv(output_path, index=False)

    def run(self, output_path: str, locations_path: str, parallel: bool, n_jobs: int):
        df, df_meta = self.extract(parallel, n_jobs)
        df = self.transform(df)
        df_meta = self.transform_meta(df_meta, df)
        self.load(df, output_path)
        self.load(df_meta, locations_path)


def run_etl(output_path: str, locations_path: str, monothread: bool, n_jobs: int):
    etl = HospETL()
    etl.run(output_path, locations_path, not monothread, n_jobs)
