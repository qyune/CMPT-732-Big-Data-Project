import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
from app import app
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

engine = create_engine(
    'postgresql+psycopg2://postgres:Aws_2020@database-1.cwfbooless1u.us-east-1.rds.amazonaws.com:5432/postgres')
engine.connect()

layout = html.Div([
    # html.H2('Correlation and Prediction',
    #         style={"textAlign": "left",
    #                'marginLeft': 55,
    #                'font-size': 30}
    #         ),
    dcc.Graph(id="relation-chart"),
    dcc.Graph(id="prediction-chart"),

    # live update
    dcc.Interval(
        id='interval-component',
        # updates every 30s
        interval=600 * 1000,  # in milliseconds
        n_intervals=0
    )
])


@app.callback(Output(component_id='relation-chart', component_property='figure'),
              Input(component_id='interval-component', component_property='n_intervals'))
def update_line_chart(continents):
    df = pd.read_sql_table('graph_3_1', engine)

    fig = px.scatter(df,
                     x="AverageTemperature", y="FloodOccurrences", trendline="ols",
                     labels={
                         "FloodOccurrences": "Occurrences of Floods",
                         "AverageTemperature": "Average Temperature"
                     },
                     )
    fig.update_layout(
        margin=dict(t=120, b=50),
        title_text="Correlation between Flood and Average Temperature",
        font_size=18,
        # font_color="black",
        title_font_color="black",
        autosize=False,
        width=1200,
        height=600,
    )
    return fig


@app.callback(Output(component_id='prediction-chart', component_property='figure'),
              Input(component_id='interval-component', component_property='n_intervals'))
def update_line_chart(continents):
    df = pd.read_sql_table('graph_3_2', engine)
    data = df['count'].to_numpy()
    data = data.reshape(-1, 2)
    x_axis = np.unique(df['prediction'])
    x_axis = np.sort(x_axis)[::-1]
    y_axis = np.unique(df['label'])
    y_axis = np.sort(y_axis)[::-1]

    fig = px.imshow(data,
                    labels=dict(x="Predicted label", y="True label", color="Counts"),
                    x=x_axis,
                    y=y_axis,
                    color_continuous_scale=px.colors.sequential.Blues
                    )

    fig.update_layout(
        margin=dict(l=50, r=50, t=120, b=20),
        title_text="Model Performance by Confusion Matrix",
        font_size=18,
        # font_color="black",
        title_font_color="black",
        autosize=False,
        width=800,
        height=500,
    )
    return fig
