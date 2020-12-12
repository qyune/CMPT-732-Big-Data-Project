import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
from app import app
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine(
    'postgresql+psycopg2://postgres:Aws_2020@database-1.cwfbooless1u.us-east-1.rds.amazonaws.com:5432/postgres')
engine.connect()

df_2_4 = pd.read_sql_table('graph_2_4', engine)
df_2_4['Continent'].replace(
    {'SA': 'South America', 'EU': 'Europe', 'NA': 'North America',
     'AS': 'Asia', 'OC': 'Oceania', 'AF': 'Africa'}, inplace=True)
all_continents = df_2_4['Continent'].unique()

layout = html.Div(
    children=[
        # html.H2('Temperature Map',
        #         style={"textAlign": "left",
        #                'marginLeft': 55,
        #                'font-size': 30}
        #         ),
        dcc.Graph(id='temperature_rise', figure={}),

        dcc.Checklist(
            id="checklist",
            options=[
                {"label": x, "value": x}
                for x in all_continents
            ],
            value=all_continents[3:4],
            labelStyle={'display': 'inline-block'},
            style={"textAlign": "right"}
        ),
        dcc.Graph(id="scatter-chart"),

        dcc.Graph(id='avg_temp_trend', figure={}),

        html.Div(children=[
            dcc.Graph(id='top_countries', figure={}),

        ]),

        # live update
        dcc.Interval(
            id='interval-component',
            # updates every 30s
            interval=600 * 1000,  # in milliseconds
            n_intervals=0
        )
    ])


@app.callback(Output(component_id='temperature_rise', component_property='figure'),
              Input(component_id='interval-component', component_property='n_intervals'))
def update_graph_live(n):
    df = pd.read_sql_table('graph_2_1', engine)
    fig = px.choropleth(df,
                        locations="Code", locationmode='ISO-3',
                        color="Diff",
                        # center=dict(lat=20, lon=-120),
                        hover_name="Country",  # column to add to hover information
                        color_continuous_scale=px.colors.sequential.RdBu_r,
                        range_color=(-10, 10),
                        height=700,
                        labels={'Diff': 'Avg Rise'},
                        )
    fig.update_geos(fitbounds="locations", visible=False)

    fig.update_layout(
        title_text="Level of Changes in Global Temperature",
        font_size=18,
        # font_color="black",
        title_font_color="black",
        margin=dict(t=120)
    )
    return fig


@app.callback(
    Output(component_id='scatter-chart', component_property='figure'),
    [Input(component_id='interval-component', component_property='n_intervals'),
     Input(component_id="checklist", component_property="value")]
)
def update_scatter_chart(n, continents):
    mask = df_2_4['Continent'].isin(continents)
    fig = px.scatter(df_2_4[mask],
                     x="Year", y="AverageTemperature", color='Country',
                     labels={
                         "AverageTemperature": "Average Temperature",
                     },
                     )

    fig.update_layout(
        title_text='Average Temperature by Countries(1985 ~ 2020)',
        font_size=18,
        # font_color="black",
        title_font_color="black",
    )
    return fig


@app.callback(
    Output(component_id='avg_temp_trend', component_property='figure'),
    Input(component_id='interval-component', component_property='n_intervals'))
def update_graph_live(n):
    # obj = client.get_object(Bucket='climate-data-732', Key='FloodArchive_code.csv')
    # df = pd.read_csv(io.BytesIO(obj['Body'].read()))

    df = pd.read_sql_table('graph_2_2', engine)
    fig = px.line(df, x="Year", y="Avg_Temp_World",
                  labels={
                      "Avg_Temp_World": "Average Temperature",
                  },
                  height=600)

    fig.update_layout(
        title_text="Trend in Global Average Temperature (1985 ~ 2020)",
        font_size=18,
        # font_color="black",
        title_font_color="black",
    )
    return fig


@app.callback(Output(component_id='top_countries', component_property='figure'),
              Input(component_id='interval-component', component_property='n_intervals'))
def update_graph_live(n):
    # obj = client.get_object(Bucket='climate-data-732', Key='FloodArchive_code.csv')
    # df = pd.read_csv(io.BytesIO(obj['Body'].read()))

    df = pd.read_sql_table('graph_2_3', engine)
    df = df.loc[0:31, :]
    df.head()
    fig = px.bar(df, x='Country', y='Diff',
                 labels={
                     "Diff": "Temperature Changes",
                 },
                 height=600)

    fig.update_layout(
        title_text='Top 30 Countries by Temperature Changes',
        font_size=18,
        # font_color="black",
        title_font_color="black",

    )
    return fig
