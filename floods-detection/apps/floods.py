import pandas as pd
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
from app import app
from sqlalchemy import create_engine

engine = create_engine(
    'postgresql+psycopg2://postgres:Aws_2020@database-1.cwfbooless1u.us-east-1.rds.amazonaws.com:5432/postgres')
engine.connect()

layout = html.Div([
    # html.H2('Flood Map',
    #         style={"textAlign": "left",
    #                'marginLeft': 55,
    #                'font-size': 30}),
    dcc.Graph(id='floods', figure={}),
    dcc.Graph(id='Reasons', figure={}),
    dcc.Graph(id='Country_Rank', figure={}),
    # live update
    dcc.Interval(
        id='interval-component',
        # updates every 60s
        interval=600 * 1000,  # in milliseconds
        n_intervals=0
    )
])


@app.callback(Output(component_id='floods', component_property='figure'),
              Input(component_id='interval-component', component_property='n_intervals'))
def update_graph_live(n):
    df = pd.read_sql_table('graph_1_1', engine)

    fig = px.density_mapbox(df,
                            lat='lat', lon='long', z='Severity', radius=5,
                            hover_name='Country', zoom=1,
                            # color_continuous_scale=px.colors.sequential.Oranges,
                            color_continuous_scale=px.colors.sequential.Blues,
                            center=dict(lat=20, lon=-120),
                            mapbox_style="stamen-terrain",
                            height=700)
    fig.update_layout(
        title_text='Flood Map: Locations and Severities (1985 ~ 2020)',
        font_color="black",
        font_size=18,
        title_font_color="black",
        margin=dict(t=120)
    )
    return fig


@app.callback(Output(component_id='Reasons', component_property='figure'),
              Input(component_id='interval-component', component_property='n_intervals'))
def update_graph_live(n):
    df = pd.read_sql_table('graph_1_2', engine)
    df = df[df['count'] > 50]
    fig = px.bar(df, orientation='h',
                 x="count", y="Decade", color='SplitedCause',
                 color_continuous_scale=px.colors.sequential.Inferno,
                 labels={
                     "SplitedCause": "Causes",
                     'count': 'Counts'},
                 )
    fig.update_layout(
        title_text='What Causes Floods?',
        font_size=18,
        # font_color="black",
        title_font_color="black",
    )

    return fig


@app.callback(Output(component_id='Country_Rank', component_property='figure'),
              Input(component_id='interval-component', component_property='n_intervals'))
def update_graph_live(n):
    df = pd.read_sql_table('graph_1_3', engine)

    df = df.sort_values(by=['count'], ascending=False)
    df = df.head(30)
    fig = px.bar(df,
                 x="Country", y="count",
                 labels={
                     'count': 'Occurrences of Floods'},
                 )
    fig.update_layout(
        title_text='Top 30 Countries by Occurrences of Floods',
        font_size=18,
        # font_color="black",
        title_font_color="black",
    )
    return fig
