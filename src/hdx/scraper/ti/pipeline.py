#!/usr/bin/python
"""Ti scraper"""

import logging
from datetime import datetime

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.country import Country
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir

    def get_data_by_country(self) -> dict:
        """
        Get data from Transparency International API and split by country
        """
        base_url = self._configuration["base_url"]
        data = self._retriever.download_json(base_url)

        # Split data by country and only take data from 2012 onward
        country_data = {}
        min_year = 2012
        for record in data:
            if record["year"] >= min_year:
                country_data.setdefault(record["iso3"], []).append(record)
        return country_data

    def generate_dataset(self, countryiso: str, records: list) -> Dataset | None:
        country_name = Country.get_country_name_from_iso3(countryiso)
        if not country_name:
            logger.error(f"Couldn't find country name for {countryiso}, skipping")
            return None
        dataset_title = f"{country_name} - {self._configuration['title']}"
        dataset_name = f"{countryiso.lower()}-corruption-perceptions-index"

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )
        dataset.add_tags(self._configuration["tags"])
        try:
            dataset.add_country_location(countryiso)
        except HDXError:
            logger.error(f"Couldn't find country {countryiso}, skipping")
            return None

        records = sorted(records, key=lambda r: r["year"], reverse=True)
        years = [record["year"] for record in records]
        dataset.set_time_period(datetime(min(years), 1, 1), datetime(max(years), 1, 1))

        resource_name = f"{countryiso.lower()}_cpi.csv"
        resource_description = self._configuration["description"].replace(
            "(country)", country_name
        )
        dataset.generate_resource(
            folder=self._tempdir,
            filename=resource_name,
            rows=records,
            resourcedata={
                "name": resource_name,
                "description": resource_description,
            },
            headers=list(records[0].keys()),
        )

        return dataset

    def generate_global_dataset(self, country_data: dict) -> Dataset | None:
        all_records = [
            record for records in country_data.values() for record in records
        ]
        all_records = sorted(all_records, key=lambda r: (-r["year"], r["country"]))

        years = [record["year"] for record in all_records]
        dataset = Dataset(
            {
                "name": "global-corruption-perceptions-index",
                "title": f"Global - {self._configuration['title']}",
            }
        )
        dataset.set_time_period(datetime(min(years), 1, 1), datetime(max(years), 1, 1))
        dataset.add_tags(self._configuration["tags"])
        try:
            dataset.add_other_location("world")
        except HDXError:
            logger.error("Couldn't add world location, skipping global dataset")
            return None

        dataset.generate_resource(
            folder=self._tempdir,
            filename="global-cpi.csv",
            rows=all_records,
            resourcedata={
                "name": "global-cpi.csv",
                "description": self._configuration["description_global"],
            },
            headers=list(all_records[0].keys()),
        )

        return dataset
