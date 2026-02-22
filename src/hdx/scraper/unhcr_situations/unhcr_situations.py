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

    def get_data_from_hdx(self, dataset_name):
        dataset = Dataset.read_from_hdx(dataset_name)
        if not dataset:
            return
        resources = dataset.get_resources()
        resource_url = resources[0]["url"]
        _, iterator = self.retriever.get_tabular_rows(resource_url, dict_form=True)
        for row in iterator:
            self.old_data.append(row)

    def get_data_from_unhcr(self, geo_ids=None):
        base_url = self.configuration["base_url"]
        population_collections = self.configuration["population_collections"]
        if not geo_ids:
            geo_ids = self.configuration["geo_ids"]

        for geo_id in geo_ids:
            url = f"{base_url}?geo_id={geo_id}&population_collection={','.join(population_collections)}"
            try:
                json = self.retriever.download_json(url)
            except DownloadError:
                self.errors.add(f"Could not download data for {geo_id}")
                continue

            for data_row in json["data"]:
                country = data_row["geomaster_name"]
                iso3 = Country.get_iso3_country_code_fuzzy(country)
                if not iso3:
                    logger.warning(f"Could not find iso3 for {country}")
                country_origin = data_row["pop_origin_name"]
                if not country_origin:
                    continue
                origin_iso3 = Country.get_iso3_country_code_fuzzy(country_origin)
                if not origin_iso3:
                    logger.warning(f"Could not find iso3 for {country_origin}")
                row = {
                    "Country": country,
                    "ISO3": iso3[0],
                    "Country of Origin": country_origin,
                    "ISO3 of Origin": origin_iso3[0],
                    "Population type": data_row["pop_type_name"],
                    "Source": data_row["source"],
                    "Date": data_row["date"],
                    "Individuals": data_row["individuals"],
                }

                if row in self.old_data:
                    continue

                self.new_data.append(row)

    def generate_dataset(self):
        if len(self.new_data) == 0:
            return None
        rows = self.old_data + self.new_data
        rows = sorted(
            rows, key=lambda x: (x["ISO3"], x["Country of Origin"], x["Date"])
        )
        name = self.configuration["dataset_name"]
        title = self.configuration["dataset_title"]
        dataset = Dataset({"name": slugify(name), "title": title})
        dataset.set_maintainer("ac47b0c8-548b-4c37-a685-7377e75aad55")
        dataset.set_organization("abf4ca86-8e69-40b1-92f7-71509992be88")
        locations = [row["ISO3"] for row in rows if row["ISO3"]]
        locations = list(set(locations))
        locations.sort()
        dataset.add_country_locations(locations)
        dataset.add_tags(self.configuration["tags"])
        dates = [parse_date(row["Date"]) for row in rows]
        dataset.set_time_period(min(dates), max(dates))

        filename = f"{name.lower()}.csv"
        resourcedata = {
            "name": filename,
            "description": "Country level refugees and asylum seekers over time",
        }

        dataset.generate_resource(
            self.folder,
            filename,
            rows,
            resourcedata,
            list(rows[-1].keys()),
        )

        return dataset
