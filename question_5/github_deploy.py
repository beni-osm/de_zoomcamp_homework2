from etl_web_to_gcs import etl_web_to_gcs
from prefect.deployment import Deployment
from prefect.filesystems import GitHub

github_block = GitHub.load("github-zoomcamp-hw")

deployment = Deployment(
    flow = etl_web_to_gcs,
    name = "Github Deployment",
    storage=github_block
)
if __name__ == "__main__":
    deployment.apply()
