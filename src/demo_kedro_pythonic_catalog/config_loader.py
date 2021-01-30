import inspect
from typing import Any, Dict
from kedro.config.templated_config import TemplatedConfigLoader
from demo_kedro_pythonic_catalog.catalog import CATALOG


def _get_pythonic_catalog_config() -> Dict[str, Any]:
    """Parse datasets defined as Pythonic classes from catalog.py"""
    catalog_config = {}
    for dataset_definition_class in CATALOG:
        dataset_conf = {}
        dataset_name = getattr(dataset_definition_class, "name")
        for field_name, field_value in inspect.getmembers(dataset_definition_class):
            # ignore all functions, methods and magic members of the class
            if (
                inspect.isroutine(field_value)
                or field_name.startswith("__")
                or field_name == "name"
            ):
                continue

            # turn the dataset type into a string
            if field_name == "type":
                dataset_conf[
                    field_name
                ] = f"{field_value.__module__}.{field_value.__name__}"
            else:
                dataset_conf[field_name] = field_value

        catalog_config[dataset_name] = dataset_conf
    return catalog_config


class PythonicCatalogConfigLoader(TemplatedConfigLoader):
    """Custom ConfigLoader to load datasets defined as Pythonic classes from catalog.py"""

    def get(self, *patterns: str) -> Dict[str, Any]:
        """
        Overwrite the `get` method to read catalog configuration
        from the catalog.py module and merge it with the default YAML configuration.
        """
        #  when parsing catalog configuration, parse additional pythonic catalog configuration
        # and merge it with the default
        if "catalog*" in patterns:
            pythonnic_catalog_config = _get_pythonic_catalog_config()
        else:
            pythonnic_catalog_config = {}

        default_config = super().get(*patterns)
        default_config.update(pythonnic_catalog_config)
        return default_config
