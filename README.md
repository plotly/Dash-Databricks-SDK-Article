# Dash + Databricks SDK Article

## Links 

____.medium.com

![jobsapi](https://github.com/plotly/Databricks-Dash-SDK-Article/assets/49540501/1895b720-0974-4a07-8273-2ed152a5a871)

## Background
This article utilizes the Databricks SDK in tandem with dash to invoke the Databricks Jobs API. The Databricks notebook runs a forecasting model, then outputs and returns the forecast Plotly chart to the dash app.

## Instructions

1. Use **git clone repo.git** to clone this repository to your local filesystem.
2. Upload the **Databricks-SDK-Dash-Jobs-Notebook.ipynb** file to Databricks.
3. Ensure that you have permissions on Databricks to kick of Jobs via the Jobs API.
4. Ensure that you have a .databrickscfg file that contains your Databricks domain and PAT.
5. Ensure that your .env file contains an accurate cluster id. 
6. If the notebook title is modified, update constants.py to reflect the new name with the **notebook_name** python variable.
7. run **pip install -r requirements.txt**
8. run **app.py**


