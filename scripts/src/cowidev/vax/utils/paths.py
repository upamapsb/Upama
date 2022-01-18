import os


class Paths:
    def __init__(self, project_dir):
        self.project_dir = project_dir

    @property
    def pub_data(self):
        return os.path.join(self.project_dir, "public", "data")

    @property
    def pub_tsp(self):
        return os.path.join(self.pub_data, "internal", "timestamp")

    @property
    def pub_vax(self):
        return os.path.join(self.pub_data, "vaccinations")

    def pub_vax_loc(self, location):
        return os.path.join(self.pub_vax, "country_data", f"{location}.csv")

    @property
    def pub_vax_metadata_man(self):
        return os.path.join(self.pub_vax, "locations-manufacturer.csv")

    @property
    def pub_vax_metadata_age(self):
        return os.path.join(self.pub_vax, "locations-age.csv")

    @property
    def in_us_states(self):
        return os.path.join(self.tmp_inp, "cdc", "vaccinations")

    @property
    def tmp(self):
        return os.path.join(self.project_dir, "scripts")

    @property
    def tmp_tmp(self):
        return os.path.join(self.tmp, "scripts")

    @property
    def tmp_inp(self):
        return os.path.join(self.tmp, "input")

    @property
    def tmp_vax_out_dir(self):
        return os.path.join(self.tmp, "output", "vaccinations")

    @property
    def tmp_vax_all(self):
        return os.path.join(self.tmp, "vaccinations.preliminary.csv")

    @property
    def tmp_met_all(self):
        return os.path.join(self.tmp, "metadata.preliminary.csv")

    @property
    def tmp_html(self):
        return os.path.join(self.tmp_vax_out_dir, "source_table.html")

    @property
    def tmp_vax_metadata(self):
        return os.path.join(self.tmp_vax_out_dir, "metadata")

    @property
    def tmp_vax_metadata_age(self):
        return os.path.join(self.tmp_vax_metadata, "locations-age.csv")

    @property
    def tmp_vax_metadata_man(self):
        return os.path.join(self.tmp_vax_metadata, "locations-manufacturer.csv")

    def tmp_vax_out(self, location):
        return os.path.join(self.tmp_vax_out_dir, "main_data", f"{location}.csv")

    def tmp_vax_out_proposal(self, location):
        return os.path.join(self.tmp_vax_out_dir, "proposals", f"{location}.csv")

    def tmp_vax_out_man(self, location):
        return os.path.join(self.tmp_vax_out_dir, "by_manufacturer", f"{location}.csv")

    def tmp_vax_out_by_age_group(self, location):
        return os.path.join(self.tmp_vax_out_dir, "by_age_group", f"{location}.csv")
