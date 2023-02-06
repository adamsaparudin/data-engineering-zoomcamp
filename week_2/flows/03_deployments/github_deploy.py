# from prefect.deployments import Deployment
from prefect.filesystems import GitHub

github_block = GitHub.load("as-data-engineering-zoomcamp")
github_block.get_directory("week_2")
# github_block.save("week-2")
