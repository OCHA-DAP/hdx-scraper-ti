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
                            "name": "economics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "environment",
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
                    "maintainer": "bfeeb369-fb53-4ecd-b8d2-e98b8020a1f9",
                    "owner_org": "hdx",
                    "data_update_frequency": 365,
                    "notes": "The Corruption Perception Index (CPI) scores and ranks 180 countries "
                    "and territories worldwide based on how corrupt a country’s public sector "
                    "is perceived to be by experts and business executives. It is a composite "
                    "index, a combination of at least 3 and up to 13 surveys and assessments "
                    "of corruption, collected by a variety of reputable institutions. The "
                    "results are given on a scale of 0 (highly corrupt) to 100 (very clean). "
                    "The CPI is the most widely used indicator of corruption worldwide.\n"
                    "\n"
                    "Read more about the CPI here: [https://www.transparency.org/en/cpi]"
                    "(https://www.transparency.org/en/cpi)\n",
                }

                resources = dataset.get_resources()
                assert len(resources) == 1
                assert resources[0]["name"] == "afg_cpi.csv"
                assert (
                    resources[0]["description"]
                    == "Corruption Perceptions Index score and rank for Afghanistan"
                )

                for resource in resources:
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
                            "name": "economics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "environment",
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
                    "maintainer": "bfeeb369-fb53-4ecd-b8d2-e98b8020a1f9",
                    "owner_org": "hdx",
                    "data_update_frequency": 365,
                    "notes": "The Corruption Perception Index (CPI) scores and ranks 180 countries "
                    "and territories worldwide based on how corrupt a country’s public sector "
                    "is perceived to be by experts and business executives. It is a composite "
                    "index, a combination of at least 3 and up to 13 surveys and assessments "
                    "of corruption, collected by a variety of reputable institutions. The "
                    "results are given on a scale of 0 (highly corrupt) to 100 (very clean). "
                    "The CPI is the most widely used indicator of corruption worldwide.\n"
                    "\n"
                    "Read more about the CPI here: [https://www.transparency.org/en/cpi]"
                    "(https://www.transparency.org/en/cpi)\n",
                }

                global_resources = global_dataset.get_resources()
                assert len(global_resources) == 1
                assert global_resources[0]["name"] == "global-cpi.csv"
                assert (
                    global_resources[0]["description"]
                    == "Corruption Perceptions Index scores and ranks for all countries"
                )

                for global_resource in global_resources:
                    filename = global_resource["name"]
                    actual = join(tempdir, filename)
                    expected = join(input_dir, filename)
                    assert_files_same(actual, expected)
