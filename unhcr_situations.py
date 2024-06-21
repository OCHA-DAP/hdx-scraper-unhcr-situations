"""
UNHCR Situations:
------------

Reads UNHCR JSONs and creates datasets.

"""
import logging

from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import DownloadError
from slugify import slugify

logger = logging.getLogger(__name__)


class UNHCRSituations:
    def __init__(self, configuration, retriever, folder, errors):
        self.configuration = configuration
        self.retriever = retriever
        self.folder = folder
        self.old_data = []
        self.new_data = []
        self.errors = errors

    def get_data_from_hdx(self):
        dataset = Dataset.read_from_hdx(self.configuration["dataset_name"])
        if not dataset:
            return
        resources = dataset.get_resources()
        resource_url = resources[0]["url"]
        _, iterator = self.retriever.get_tabular_rows(resource_url, dict_form=True)
        for row in iterator:
            self.old_data.append(row)

    def get_data_from_unhcr(self):
        base_url = self.configuration["base_url"]
        population_collections = self.configuration["population_collections"]
        sv_ids = self.configuration["sv_ids"]

        for sv_id in sv_ids:
            url = f"{base_url}?sv_id={sv_id}&population_collection={','.join(population_collections)}"
            try:
                json = self.retriever.download_json(url)
            except DownloadError:
                self.errors.add(f"Could not download data for {sv_id}")
                continue

            for data_row in json["data"]:
                country = data_row["geomaster_name"]
                if country == "Other":
                    continue
                iso3 = Country.get_iso3_country_code_fuzzy(country)
                if not iso3:
                    logger.warning(f"Could not find iso3 for {country}")
                row = {
                    "country": country,
                    "iso3": iso3[0],
                    "source": data_row["source"],
                    "date": data_row["date"],
                    "individuals": data_row["individuals"],
                }

                if row in self.old_data:
                    continue

                self.new_data.append(row)

    def generate_dataset(self):
        if len(self.new_data) == 0:
            return None
        rows = self.old_data + self.new_data
        name = self.configuration["dataset_name"]
        title = self.configuration["dataset_title"]
        dataset = Dataset({"name": slugify(name), "title": title})
        dataset.set_maintainer("ac47b0c8-548b-4c37-a685-7377e75aad55")
        dataset.set_organization("abf4ca86-8e69-40b1-92f7-71509992be88")
        locations = [row["iso3"] for row in rows if row["iso3"]]
        locations = list(set(locations))
        dataset.add_country_locations(locations)
        dataset.add_tags(self.configuration["tags"])
        dates = [parse_date(row["date"]) for row in rows]
        dataset.set_time_period(min(dates), max(dates))

        filename = f"{name.lower()}.csv"
        resourcedata = {
            "name": filename,
            "description": "Country level refugees, asylum seekers, and others of concern over time",
        }

        dataset.generate_resource_from_rows(
            self.folder,
            filename,
            rows,
            resourcedata,
            list(rows[0].keys()),
        )

        return dataset
