import pandas as pd


class OxCGRTETL:
    def __init__(self) -> None:
        self.source_url = "https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_latest.csv"

    def extract(self):
        return pd.read_csv(self.source_url, low_memory=False)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def load(self, df: pd.DataFrame, output_path: str):
        df.to_csv(output_path, index=False)

    def run(self, output_path: str):
        df = self.extract()
        self.load(df, output_path)


def run_etl(output_path: str):
    etl = OxCGRTETL()
    etl.run(output_path)
