## Question 4
- `python week_2/03_deployments/github_deploy.py`
- `prefect deployment build week_2/etl_web_to_gcs.py:etl_web_to_gcs_parent --name etl_web_gcs_gh -sb github/as-data-engineering-zoomcamp -a`
- `prefect deployment run etl-web-to-gcs-parent/etl_web_gcs_gh -p "months=[11]" -p year=2020 -p color=green`