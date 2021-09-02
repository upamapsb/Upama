import os

from ._config_loader import vaccines_mapping, countries_mapping

__CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))

# WHO
__WHO_CONFIG = os.path.join(__CURRENT_DIR, "who_config.yaml")
WHO_VACCINES = vaccines_mapping(__WHO_CONFIG)
WHO_COUNTRIES = countries_mapping(__WHO_CONFIG)

# PAHO
__PAHO_CONFIG = os.path.join(__CURRENT_DIR, "paho_config.yaml")
PAHO_VACCINES = vaccines_mapping(__PAHO_CONFIG)
PAHO_COUNTRIES = countries_mapping(__PAHO_CONFIG)

# AFRICACDC
__AFRICACDC_CONFIG = os.path.join(__CURRENT_DIR, "acdc_config.yaml")
ACDC_VACCINES = vaccines_mapping(__AFRICACDC_CONFIG)
ACDC_COUNTRIES = countries_mapping(__AFRICACDC_CONFIG)
