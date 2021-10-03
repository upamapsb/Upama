import yaml


class ConfigLoader:
    def __init__(self, config: dict) -> None:
        self.config = config

    @classmethod
    def from_yaml(cls, path: str):
        with open(path, "r") as f:
            config = yaml.safe_load(f)
        return cls(config)

    def vaccines_mapping(self):
        if "vaccines" not in self.config:
            return {}
        return _records_to_dict_many(self.config["vaccines"])

    def countries_mapping(self):
        if "countries" not in self.config:
            return {}
        return _records_to_dict(self.config["countries"])

    def list_countries(self):
        return list(self.countries_mapping().values())


def _records_to_dict_many(records):
    dix = {}
    for record in records:
        who_name = record["org_name"]
        owid_name = record["owid_name"]
        if isinstance(who_name, list):
            for n in who_name:
                dix[n] = owid_name
        else:
            dix[who_name] = owid_name
    return dix


def _records_to_dict(records):
    return {record["org_name"]: record["owid_name"] for record in records}


def vaccines_mapping(config_path):
    """Get organization's vaccine mapping.

    Returns:
        dict: ORG_VACCINE_NAME -> OWID_VACCINE_NAME
    """
    config_loader = ConfigLoader.from_yaml(config_path)
    return config_loader.vaccines_mapping()


def countries_mapping(config_path):
    """Get organization's country mapping.

    Note, only contains mappings for countries for which we source data from the organization.

    Returns:
        dict: ORG_COUNTRY_NAME -> OWID_COUNTRY_NAME
    """
    config_loader = ConfigLoader.from_yaml(config_path)
    return config_loader.countries_mapping()


def countries(config_path):
    """List countries for which we source data from the organization.

    Returns:
        list: List of countries sourced from the organization.
    """
    config_loader = ConfigLoader.from_yaml(config_path)
    return config_loader.config


def get_org_constants(config_file: str):
    """Get organisation's constants (country + vaccine mapping)

    Args:
        config_file (str): Path to config file (YAML)

    Returns:
        tuple: Country and vaccine mapping
    """
    return countries_mapping(config_file), vaccines_mapping(config_file)
