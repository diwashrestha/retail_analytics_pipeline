from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def load_yaml(path: Path):
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def test_bundle_name():
    config = load_yaml(ROOT / "databricks.yml")

    assert config["bundle"]["name"] == "einkaufpark-retail-platform"


def test_dev_is_default_target():
    config = load_yaml(ROOT / "databricks.yml")

    assert config["targets"]["dev"]["default"] is True
    assert config["targets"]["dev"]["mode"] == "development"


def test_release_is_production_mode():
    config = load_yaml(ROOT / "databricks.yml")

    assert config["targets"]["release"]["mode"] == "production"


def test_medallion_job_exists():
    config = load_yaml(ROOT / "resources" / "retail_job.yml")

    assert "retail_medallion_job" in config["resources"]["jobs"]


def test_pipeline_never_full_refreshes_incrementally():
    config = load_yaml(ROOT / "resources" / "retail_job.yml")

    job = config["resources"]["jobs"]["retail_medallion_job"]

    pipeline_tasks = [task for task in job["tasks"] if "pipeline_task" in task]

    assert len(pipeline_tasks) == 1

    assert pipeline_tasks[0]["pipeline_task"]["full_refresh"] is False


def test_medallion_job_dependency_order():
    config = load_yaml(ROOT / "resources" / "retail_job.yml")

    tasks = config["resources"]["jobs"]["retail_medallion_job"]["tasks"]

    by_key = {task["task_key"]: task for task in tasks}

    assert {
        "generate_retail_data",
        "refresh_bronze_silver_gold",
        "validate_medallion",
    }.issubset(by_key)

    pipeline_dependencies = {
        item["task_key"] for item in by_key["refresh_bronze_silver_gold"]["depends_on"]
    }

    assert pipeline_dependencies == {"generate_retail_data"}

    validation_dependencies = {
        item["task_key"] for item in by_key["validate_medallion"]["depends_on"]
    }

    assert validation_dependencies == {"refresh_bronze_silver_gold"}
