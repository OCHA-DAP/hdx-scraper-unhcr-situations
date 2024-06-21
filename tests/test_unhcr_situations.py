#!/usr/bin/python
"""
Unit tests for unhcr situations.

"""
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.vocabulary import Vocabulary
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from unhcr_situations import UNHCRSituations


class TestUNHCRSituations:
    dataset = {
        "name": "unhcr-situations",
        "title": "UNHCR Situations: Refugees, Asylym Seekers, and Others of Concern",
        "maintainer": "ac47b0c8-548b-4c37-a685-7377e75aad55",
        "owner_org": "abf4ca86-8e69-40b1-92f7-71509992be88",
        "groups": [
            {"name": "afg"}, {"name": "ago"}, {"name": "bdi"}, {"name": "ben"}, {"name": "bfa"}, {"name": "bgd"},
            {"name": "bwa"}, {"name": "caf"}, {"name": "civ"}, {"name": "cmr"}, {"name": "cod"}, {"name": "cog"},
            {"name": "com"}, {"name": "dji"}, {"name": "eri"}, {"name": "eth"}, {"name": "gab"}, {"name": "gha"},
            {"name": "gin"}, {"name": "gmb"}, {"name": "gnb"}, {"name": "idn"}, {"name": "ind"}, {"name": "irn"},
            {"name": "kaz"}, {"name": "ken"}, {"name": "kgz"}, {"name": "lbr"}, {"name": "lso"}, {"name": "mdg"},
            {"name": "mli"}, {"name": "moz"}, {"name": "mus"}, {"name": "mwi"}, {"name": "mys"}, {"name": "nam"},
            {"name": "ner"}, {"name": "nga"}, {"name": "pak"}, {"name": "rwa"}, {"name": "sdn"}, {"name": "sen"},
            {"name": "sle"}, {"name": "som"}, {"name": "ssd"}, {"name": "swz"}, {"name": "tcd"}, {"name": "tgo"},
            {"name": "tha"}, {"name": "tjk"}, {"name": "tkm"}, {"name": "tza"}, {"name": "uga"}, {"name": "uzb"},
            {"name": "zaf"}, {"name": "zmb"}, {"name": "zwe"},
        ],
        "tags": [
            {"name": "asylum seekers", "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1"},
            {"name": "refugees", "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1"},
        ],
        "dataset_date": "[2023-12-31T00:00:00 TO 2024-06-21T23:59:59]",
        "package_creator": "briar-mills",
        "dataset_source": "UNHCR",
        "data_update_frequency": "30",
        "license_id": "cc-by-igo",
        "caveats": "Data is operational and may not match yearly official figures",
        "subnational": "0",
        "methodology": "Other",
        "methodology_other": "Collected from various sources",
        "private": False,
        "notes": "Contains data from the UNHCR situations [operational data portal](https://data.unhcr.org/en/situations) covering refugees, asylum seekers, and others of concern.",
    }

    resource = {
        "name": "unhcr-situations.csv",
        "description": "Country level refugees, asylum seekers, and others of concern over time",
        "format": "csv",
        "resource_type": "file.upload",
        "url_type": "upload",
    }

    @pytest.fixture(scope="function")
    def fixtures(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            hdx_read_only=True,
            user_agent="test",
            project_config_yaml=join("config", "project_configuration.yaml"),
        )
        UserAgent.set_global("test")
        tags = (
            "asylum seekers",
            "refugees",
        )
        Vocabulary._tags_dict = {tag: {"Action to Take": "ok"} for tag in tags}
        tags = [{"name": tag} for tag in tags]
        Vocabulary._approved_vocabulary = {
            "tags": tags,
            "id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            "name": "approved",
        }
        return Configuration.read()

    def test_main(self, configuration, fixtures):
        with temp_dir(
                "test_unhcr_situations", delete_on_success=True, delete_on_failure=False
        ) as folder:
            with Download() as downloader:
                retriever = Retrieve(downloader, folder, fixtures, folder, False, True)
                unhcr_situations = UNHCRSituations(configuration, retriever, folder, ErrorsOnExit())
                dataset = Dataset.load_from_json(join(fixtures, f"{configuration['dataset_name']}.json"))
                unhcr_situations.get_data_from_hdx(dataset)
                assert len(unhcr_situations.old_data) == 4
                assert unhcr_situations.old_data[0] == {
                    "Country": "Democratic Republic of the Congo", "ISO3": "COD", "Source": "UNHCR, Government", "Date": "2024-05-31", "Individuals": "526248"
                }
                unhcr_situations.get_data_from_unhcr()
                assert len(unhcr_situations.new_data) == 53
                assert unhcr_situations.new_data[0] == {
                    "Country": "South Africa", "ISO3": "ZAF", "Source": "UNHCR, Government", "Date": "2024-04-30", "Individuals": "154122"
                }
                dataset = unhcr_situations.generate_dataset()
                dataset.update_from_yaml()
                assert dataset == self.dataset
                resources = dataset.get_resources()
                assert resources[0] == self.resource
                file = f"{configuration['dataset_name']}.csv"
                assert_files_same(join(fixtures, file), join(folder, file))
