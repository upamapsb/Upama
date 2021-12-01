from datetime import datetime
from dataclasses import dataclass
from typing import Callable

import pandas as pd


@dataclass
class Grapheriser:
    location: str = "location"
    date: str = "date"
    date_ref: datetime = datetime(2020, 1, 21)
    fillna: bool = False
    fillna_0: bool = True
    pivot_column: str = None
    pivot_values: str = None
    function_input: Callable = lambda x: x
    function_output: Callable = lambda x: x

    @property
    def columns_metadata(self) -> list:
        return ["Country", "Year"]

    def columns_data(self, df: pd.DataFrame) -> list:
        return [col for col in df.columns if col not in self.columns_metadata]

    def pipe_pivot(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.pivot_column is not None and self.pivot_values is not None:
            return df.pivot(
                index=[self.location, self.date],
                columns=self.pivot_column,
                values=self.pivot_values,
            ).reset_index()
        return df

    def pipe_metadata_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df = (
            df.rename(
                columns={
                    self.location: "Country",
                }
            )
            .assign(date=(df[self.date] - self.date_ref).dt.days)
            .rename(columns={"date": "Year"})
        ).copy()
        return df

    def pipe_order_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        col_order = self.columns_metadata + self.columns_data(df)
        df = df[col_order].sort_values(col_order)
        return df

    def pipe_fillna(self, df: pd.DataFrame) -> pd.DataFrame:
        columns_data = self.columns_data(df)
        if self.fillna:
            df[columns_data] = df.groupby(["Country"])[columns_data].fillna(method="ffill")
        if self.fillna_0:
            df[columns_data] = df[columns_data].fillna(0)
        return df

    def read(self, input_path: str):
        df = pd.read_csv(input_path, parse_dates=[self.date])
        return df

    def pipeline(self, df: pd.DataFrame):
        df = (
            df.pipe(self.function_input)
            .pipe(self.pipe_pivot)
            .pipe(self.pipe_metadata_columns)
            .pipe(self.pipe_order_columns)
            .pipe(self.pipe_fillna)
            .pipe(self.function_output)
        )
        return df

    def run(self, input_path: str, output_path: str):
        df = self.read(input_path)
        df = df.pipe(self.pipeline)
        df.to_csv(output_path, index=False)
