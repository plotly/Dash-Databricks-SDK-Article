import dash
from dash import dcc, html, Input, Output, callback, State, no_update
import time
import dash_mantine_components as dmc
import plotly.graph_objs as go
import base64
import json
from constants import us_states

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

import os
from dotenv import load_dotenv

load_dotenv()

app = dash.Dash(__name__)

# https://databricks-sdk-py.readthedocs.io/en/latest/workspace/jobs.html

floating_logos = html.Div(
    [
        # GitHub logo and link
        html.A(
            href="https://github.com/",
            target="_blank",
            children=[
                html.Img(
                    src="https://github.githubassets.com/images/modules/logos_page/GitHub-Mark.png",  # Use the appropriate GitHub logo URL
                    style={"width": "70px", "height": "70px", "marginRight": "25px"},
                )
            ],
        ),
        # Medium logo and link
        html.A(
            href="https://medium.com/",
            target="_blank",
            children=[
                html.Img(
                    src="https://cdn-images-1.medium.com/max/1200/1*6_fgYnisCa9V21mymySIvA.png",  # Use the appropriate Medium logo URL
                    style={"width": "70px", "height": "70px"},
                )
            ],
        ),
    ],
    style={
        "position": "fixed",
        "bottom": "30px",
        "right": "30px",
        "zIndex": 1000,
    },
)

app.layout = html.Div(
    style={"fontFamily": "Arial, sans-serif", "padding": "1em"},
    children=[
        # Header
        dmc.Header(
            className="header",
            height="10%",
            children=[
                dmc.Image(
                    className="logo",
                    src="assets/logo.png",
                    alt="Logo",
                    width=100,
                ),
                dmc.Container(
                    children=[
                        dmc.Title(
                            className="title",
                            order=1,
                            children="Plotly Dash + Databricks",
                            color="#F9F7F4",
                            # align="center",
                        ),
                        dmc.Title(
                            className="title",
                            order=3,
                            children="Leveraging the Databricks SDK and Jobs API",
                            color="#EEEDE9",
                            # align="center",
                        ),
                    ],
                ),
                dmc.Image(
                    className="logo",
                    src="assets/databricks.png",
                    alt="Logo",
                    width=100,
                ),
            ],
        ),
        # Control Panel
        html.Div(
            style={
                "marginTop": "2em",
                "border": "1px solid #e5e5e5",
                "padding": "1em",
                "borderRadius": "5px",
                "backgroundColor": "#F9F7F4",
            },
            children=[
                dmc.LoadingOverlay(
                    id="loading-form",
                    children=[
                        dmc.Title(
                            className="title",
                            order=2,
                            children="Use this app to run a forecasting model that resides in your Databricks notebook",
                        ),
                        dmc.Space(h=15),
                        dmc.Title(
                            className="subtitle",
                            order=6,
                            children=(
                                [
                                    '1. Use the "States" dropdown to filter to a specific state\'s training data',
                                ]
                            ),
                        ),
                        dmc.Title(
                            className="subtitle",
                            order=6,
                            children=(
                                [
                                    '2. Use the "Forecast" field to determine how many days the Prophet forecasting model should guess ahead',
                                ]
                            ),
                        ),
                        dmc.Title(
                            className="subtitle",
                            order=6,
                            children=(
                                [
                                    '3. When you are happy with your inputs, press "Run Job" below and wait for the Plotly widget to appear',
                                ]
                            ),
                        ),
                        dmc.Space(h=30),
                        dmc.Group(
                            align="center",
                            grow=True,
                            spacing="5%",
                            children=[
                                dmc.Select(
                                    id="state-dropdown",
                                    label="States",
                                    data=us_states,
                                    value="All States",
                                ),
                                dmc.NumberInput(
                                    label="Number of days to forecast:",
                                    id="forecast-forward-days",
                                    value=180,
                                ),
                                dmc.Button(
                                    id="jobs-api-button",
                                    color="orange",
                                    children="Run Job",
                                    n_clicks=0,
                                ),
                            ],
                        ),
                    ],
                    loaderProps={"variant": "bars", "color": "orange", "size": "xl"},
                ),
            ],
        ),
        dmc.Space(h=20),
        dmc.Center(
            children=[
                html.Div(
                    id="forecast-plot",
                    children="No graph loaded yet",
                    style={
                        "width": "100%",
                        "height": "100%",
                        "text-align": "center",
                        "align-items": "center",
                    },
                )
            ]
        ),
        floating_logos,
    ],
)


@callback(
    Output("loading-form", "children"),
    Output("forecast-plot", "children"),
    State("state-dropdown", "value"),
    State("forecast-forward-days", "value"),
    Input("jobs-api-button", "n_clicks"),
    prevent_initial_callback=True,
)
def invoke_jobs_api(state, forecast_days, n_clicks):
    if n_clicks == 0:
        return no_update, no_update
    w = WorkspaceClient()

    params_from_dash = {"us-state": state, "forecast-forward-days": forecast_days}

    notebook_path = f"/Users/{w.current_user.me().user_name}/Jobs API Article Test"
    try:
        w.clusters.ensure_cluster_is_running(os.environ["DATABRICKS_CLUSTER_ID"])
    except:
        print(
            "Your connection to databricks isn't configured correctly. Revise your /.databrickscfg file"
        )

    created_job = w.jobs.create(
        name=f"sdk-{time.time_ns()}",
        tasks=[
            jobs.Task(
                description="Run Jobs API Notebook",
                existing_cluster_id=os.environ["DATABRICKS_CLUSTER_ID"],
                notebook_task=jobs.NotebookTask(
                    notebook_path=notebook_path, base_parameters=params_from_dash
                ),
                task_key="test",
                timeout_seconds=0,
            )
        ],
    )
    w.jobs.run_now(job_id=created_job.job_id).result()

    fig_bytes = w.dbfs.read("/tmp/forecast_plot.json")
    # Extract the content from the response
    content = fig_bytes.data

    # Decode the byte content to get a string
    decoded_content = base64.b64decode(content).decode("utf-8")

    # Now, you can use decoded_content as a regular string.
    w.jobs.delete(job_id=created_job.job_id)

    # Load the decoded content into a Python dictionary
    fig_data = json.loads(decoded_content)

    # Convert the dictionary to a Plotly Figure
    fig = go.Figure(fig_data)

    fig.update_layout(
        autosize=True,
        width=None,  # removing hardcoded width
        height=None,  # removing hardcoded height
    )

    fig.update_layout(
        # White background
        plot_bgcolor="#F9F7F4",
        paper_bgcolor="#F9F7F4",
        # Titles and fonts
        title="Forecast Results",
        title_font=dict(size=24, family="Arial, sans-serif", color="#1B3139"),
        # Axis labels
        xaxis=dict(
            title="Number of product units",
            titlefont=dict(size=18, color="#1B3139"),
            showgrid=True,
            gridcolor="lightgrey",
            gridwidth=0.5,
            zerolinecolor="lightgrey",
            zerolinewidth=0.5,
            tickfont=dict(size=14, color="#1B3139"),
        ),
        yaxis=dict(
            title="Order date",
            titlefont=dict(size=18, color="#1B3139"),
            showgrid=True,
            gridcolor="lightgrey",
            gridwidth=0.5,
            zerolinecolor="lightgrey",
            zerolinewidth=0.5,
            tickfont=dict(size=14, color="#1B3139"),
        ),
        # Legend styling
        legend=dict(font=dict(size=14, color="grey")),
    )

    # print(notebook_path, cluster_id)
    return no_update, dcc.Graph(
        figure=fig,
    )


if __name__ == "__main__":
    app.run()  # debug=True, use_reloader=True)
