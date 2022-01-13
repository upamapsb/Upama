import pandas as pd
from cowidev.gmobility.dtypes import dtype


class GMobilityETL:
    source_url = "https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv"

    def extract(self):
        return pd.read_csv(
            self.source_url,
            usecols=dtype.keys(),
            # low_memory=False,
            dtype=dtype,
        )

    def load(self, df: pd.DataFrame, output_path: str) -> None:
        # Export data
        df.to_csv(output_path, index=False)

    def run(self, output_path: str):
        df = self.extract()
        self.load(df, output_path)


def run_etl(output_path: str):
    etl = GMobilityETL()
    etl.run(output_path)
