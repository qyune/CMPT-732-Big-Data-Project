import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import datetime

# Connect to main app.py file
from app import app
from app import server

# Connect to your app pages
from apps import info, floods, temperature, prediction


def serve_layout():
    return html.Div([
        html.H1('Flood and Global Warming',
                style={'textAlign': 'left',
                       'marginLeft': 55}),
        dcc.Location(id='url', refresh=False),
        html.Div([
            dcc.Link('Flood   ', href='/apps/floods'),
            dcc.Link('Temperature   ', href='/apps/temperature'),
            dcc.Link('Correlation and Prediction', href='/apps/prediction')
            # dcc.Link('Video Games | ', href='/apps/vgames'),
            # dcc.Link('Other Products', href='/apps/global_sales')
        ], className="row"),

        html.Div(id='page-content', children=[]),
        # html.H3('The time is: ' + str(datetime.datetime.now()))
    ])


app.layout = serve_layout


@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/apps/floods':
        return floods.layout
    if pathname == '/apps/temperature':
        return temperature.layout
    if pathname == '/apps/prediction':
        return prediction.layout
    if pathname == '/':
        return info.layout

    else:
        return "404 Page Error! Please choose a link"


if __name__ == '__main__':
    app.run_server(debug=True)
