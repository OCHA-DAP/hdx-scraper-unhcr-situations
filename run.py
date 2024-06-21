#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.retriever import Retrieve
from hdx.utilities.path import temp_dir

from unhcr_situations import UNHCRSituations

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-unhcr-situations"
updated_by_script = "HDX Scraper: UNHCR Situations"


def main(save: bool = False, use_saved: bool = False) -> None:
    """Generate dataset and create it in HDX"""
    with ErrorsOnExit() as errors:
        with temp_dir(lookup) as temp_folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, temp_folder, "saved_data", temp_folder, save, use_saved
                )
                configuration = Configuration.read()
                unhcr_situations = UNHCRSituations(configuration, retriever, temp_folder, errors)
                dataset = Dataset.read_from_hdx(configuration["dataset_name"])
                unhcr_situations.get_data_from_hdx(dataset)
                unhcr_situations.get_data_from_unhcr()
                dataset = unhcr_situations.generate_dataset()
                if dataset:
                    dataset.update_from_yaml()
                    dataset["notes"] = dataset["notes"].replace(
                        "\n", "  \n"
                    )  # ensure markdown has line breaks
                    try:
                        dataset.create_in_hdx(
                            remove_additional_resources=True,
                            hxl_update=False,
                            updated_by_script=updated_by_script,
                        )
                    except HDXError:
                        errors.add("Could not upload dataset to HDX")


if __name__ == "__main__":
    facade(
        main,
        hdx_site="demo",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yaml")
    )
