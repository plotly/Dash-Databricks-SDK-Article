# Dash + Databricks SDK App Walkthrough

## Links 

https://plotlygraphs.medium.com/databricks-sdk-plotly-dash-the-easiest-way-to-get-jobs-done-70d44e1cd9c3

## App Preview

![jobsapi](https://github.com/plotly/Dash-Databricks-SDK-Article/assets/49540501/86dba58a-87a2-4c15-9064-20f27a443cfb)


## Background
In short, the purpose of this project is to demonstrate how Plotly's Dash can be utilized in tandem with the Databricks SDK. More specifically, here we choose to kick off and parameterize a Databricks notebook from a dash application, either running locally, on Heroku, or on Dash Enterprise.

For more information on background and our joint story, please [our associated Medium article](medium.com).

For information on how to get started, see the below instruction set.

## Instructions

#### On Databricks:
1. Ensure that you have permissions on Databricks to kick of Jobs via the Jobs API. Check with your Databricks workspace administrator if you do not, or if any commands in this project fail unusually.
2. Upload the **Databricks-SDK-Dash-Jobs-Notebook.ipynb** file to Databricks by clicking (**+ New**) -> Notebook (screenshot below).
![upload](https://github.com/plotly/Dash-Databricks-SDK-Article/assets/49540501/d9dc034f-98a1-4623-97d5-c8d198c2e13c)

3. Run the notebook, and attach it to the cluster that you would like to utilize. **Importantly, get the Datbricks cluster's ID**. You will utilize this in your dash app's .env file locally. [Link]([url](https://community.databricks.com/t5/data-engineering/how-do-i-get-the-current-cluster-id/td-p/28403)).

#### On your own computer:
1. Use ```git clone git@github.com:plotly/Dash-Databricks-SDK-Article.git``` to clone this repository to your local filesystem.
2. Ensure that you have a .databrickscfg file that contains your Databricks domain and PAT. By default, it should be located in your base directory. i.e.
  ```/.databrickscfg```
   The file structure should resemble the example provided in this repository, with your Databricks host name and [personal access token]([url](https://docs.databricks.com/en/administration-guide/access-control/tokens.html)https://docs.databricks.com/en/administration-guide/access-control/tokens.html).
4. ```cd``` into your project directory (called **Databricks-Dash-SDK-Article** by default)
5. Remove ```.databrickscfg``` from your project's directory, proivded you have it already at your base directory (step 2).
6. In ```.env``` file, copy-paste your cluster's ID into the ```DATABRICKS_CLUSTER_ID``` field.
7. Modify constants.py as needed. Mainly, you may choose to rename the Databricks notebook provided in this project. If so, **reflect those changes by modifying the ```notebook_name``` variable**.
8. Run ```pip install -r requirements.txt```
9. Run ```python app.py``` to get started!



