import os
import pytz
from datetime import datetime, timedelta
import traceback

from cowidev.grapher.db.utils.db_imports import import_dataset
from cowidev.grapher.db.utils.slack_client import send_error


class GrapherBaseUpdater:
    def __init__(
        self,
        dataset_name: str,
        source_name: str,
        zero_day: str,
        input_csv_path: str = None,
        slack_notifications: bool = False,
        namespace: str = "owid",
        year_is_day: bool = True,
        unit: str = "",
        unit_short: str = None,
    ) -> None:
        self.dataset_name = dataset_name
        self._input_csv_path = input_csv_path
        self.source_name = source_name
        self.zero_day = zero_day
        self.slack_notifications = slack_notifications
        self.namespace = namespace
        self.year_is_day = year_is_day
        self.unit = unit
        self.unit_short = unit_short

    @property
    def project_dir(self):
        return os.environ.get("OWID_COVID_PROJECT_DIR")

    @property
    def input_csv_path(self):
        if self._input_csv_path is not None:
            return self._input_csv_path
        if self.project_dir:
            return os.path.join(self.project_dir, "scripts", "grapher", f"{self.dataset_name}.csv")
        raise ValueError(
            "Either specify attribute `_input_csv_path` or set environment variable ${OWID_COVID_PROJECT_DIR}."
        )

    @property
    def time_str(self):
        return (
            (datetime.now() - timedelta(minutes=10))
            .astimezone(pytz.timezone("Europe/London"))
            .strftime("%-d %B %Y, %H:%M")
        )

    def run(self):
        try:
            import_dataset(
                dataset_name=self.dataset_name,
                namespace=self.namespace,
                csv_path=self.input_csv_path,
                default_variable_display={
                    "yearIsDay": self.year_is_day,
                    "zeroDay": self.zero_day,
                },
                source_name=self.source_name,
                slack_notifications=self.slack_notifications,
                unit=self.unit,
                unit_short=self.unit_short,
            )
        except Exception as e:
            tb = traceback.format_exc()
            send_error(
                channel="corona-data-updates",
                title=f"Updating Grapher dataset: {self.dataset_name}",
                trace=tb,
            )
