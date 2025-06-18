#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from os.path import dirname, expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXError
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.unhcr_situations.unhcr_situations import UNHCRSituations

logger = logging.getLogger(__name__)

_USER_AGENT_LOOKUP = "hdx-scraper-unhcr-situations"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: UNHCR Situations"


def main(save: bool = False, use_saved: bool = False) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to True.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """
    configuration = Configuration.read()
    User.check_current_user_write_access("unhcr")
    with ErrorsOnExit() as errors:
        with temp_dir(_USER_AGENT_LOOKUP) as temp_folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, temp_folder, "saved_data", temp_folder, save, use_saved
                )
                unhcr_situations = UNHCRSituations(
                    configuration, retriever, temp_folder, errors
                )
                unhcr_situations.get_data_from_hdx(configuration["dataset_name"])
                unhcr_situations.get_data_from_unhcr()
                dataset = unhcr_situations.generate_dataset()
                if dataset:
                    dataset.update_from_yaml(
                        path=join(
                            dirname(__file__), "config", "hdx_dataset_static.yaml"
                        )
                    )
                    dataset["notes"] = dataset["notes"].replace(
                        "\n", "  \n"
                    )  # ensure markdown has line breaks
                    try:
                        dataset.create_in_hdx(
                            remove_additional_resources=True,
                            match_resource_order=False,
                            hxl_update=False,
                            updated_by_script=_UPDATED_BY_SCRIPT,
                        )
                    except HDXError:
                        errors.add("Could not upload dataset to HDX")


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
        project_config_yaml=join(
            dirname(__file__), "config", "project_configuration.yaml"
        ),
    )
