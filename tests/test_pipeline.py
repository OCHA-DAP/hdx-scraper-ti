from os.path import join

from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.ti.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestTi",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                pipeline = Pipeline(configuration, retriever, tempdir)

                country_data = pipeline.get_data_by_country()

                # Records from before 2012 should be excluded
                assert "AFG" in country_data
                assert len(country_data["AFG"]) == 3

                dataset = pipeline.generate_dataset("AFG", country_data["AFG"])
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )

                assert dataset == {
                    "caveats": "None",
                    "name": "afg-corruption-perceptions-index",
                    "title": "Afghanistan - Corruption Perceptions Index",
                    "dataset_date": "[2012-01-01T00:00:00 TO 2024-01-01T23:59:59]",
                    "tags": [
                        {
                            "name": "development",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "governance and civil society",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "indicators",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "poverty",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "socioeconomics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "license_id": "cc-by",
                    "methodology": "https://images.transparencycdn.org/images/CPI-2024-Methodology.zip",
                    "dataset_source": "Transparency International",
                    "groups": [{"name": "afg"}],
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "0e37ef02-82c6-4f36-a336-52e94df45ee9",
                    "owner_org": "e0035cc4-87fb-443c-8045-4e9f7463a67c",
                    "data_update_frequency": 365,
                    "notes": "This dataset contains Corruption Perceptions Index (CPI) scores and rankings for countries and territories, published by Transparency International. Scores range from 0 (highly corrupt) to 100 (very clean) and reflect perceived levels of public sector corruption based on assessments by experts and business executives.\n"
                    "\n"
                    "For more information, visit the [Corruption Perceptions Index](https://www.transparency.org/en/cpi) on the Transparency International website.\n",
                }

                resources = dataset.get_resources()
                assert len(resources) == 1
                assert resources[0]["name"] == "afg_cpi.csv"
                assert (
                    resources[0]["description"]
                    == "Corruption Perceptions Index score and rank for Afghanistan"
                )

                data_dictionary = configuration["data_dictionary"]
                for resource in resources:
                    assert resource.get_hdx_data_dictionary() == data_dictionary
                    filename = resource["name"]
                    actual = join(tempdir, filename)
                    expected = join(input_dir, filename)
                    assert_files_same(actual, expected)

                global_dataset = pipeline.generate_global_dataset(country_data)
                global_dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )

                assert global_dataset == {
                    "caveats": "None",
                    "name": "global-corruption-perceptions-index",
                    "title": "Global - Corruption Perceptions Index",
                    "dataset_date": "[2012-01-01T00:00:00 TO 2024-01-01T23:59:59]",
                    "tags": [
                        {
                            "name": "development",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "governance and civil society",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "indicators",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "poverty",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "socioeconomics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "license_id": "cc-by",
                    "methodology": "https://images.transparencycdn.org/images/CPI-2024-Methodology.zip",
                    "dataset_source": "Transparency International",
                    "groups": [{"name": "world"}],
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "0e37ef02-82c6-4f36-a336-52e94df45ee9",
                    "owner_org": "e0035cc4-87fb-443c-8045-4e9f7463a67c",
                    "data_update_frequency": 365,
                    "notes": "This dataset contains Corruption Perceptions Index (CPI) scores and rankings for countries and territories, published by Transparency International. Scores range from 0 (highly corrupt) to 100 (very clean) and reflect perceived levels of public sector corruption based on assessments by experts and business executives.\n"
                    "\n"
                    "For more information, visit the [Corruption Perceptions Index](https://www.transparency.org/en/cpi) on the Transparency International website.\n",
                }

                global_resources = global_dataset.get_resources()
                assert len(global_resources) == 4  # 1 all-years + 3 per-year
                assert global_resources[0]["name"] == "global_cpi_all.csv"
                assert (
                    global_resources[0]["description"]
                    == "Corruption Perceptions Index scores and ranks for all countries"
                )
                assert global_resources[1]["name"] == "global_cpi_2024.csv"
                assert (
                    global_resources[1]["description"]
                    == "Corruption Perceptions Index scores and ranks for all countries in 2024"
                )
                assert global_resources[2]["name"] == "global_cpi_2023.csv"
                assert global_resources[3]["name"] == "global_cpi_2012.csv"

                for global_resource in global_resources:
                    assert global_resource.get_hdx_data_dictionary() == data_dictionary
                    filename = global_resource["name"]
                    actual = join(tempdir, filename)
                    expected = join(input_dir, filename)
                    assert_files_same(actual, expected)
