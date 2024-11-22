import dash
from dash import html, dcc
from dash.dependencies import Input, Output

app = dash.Dash(__name__)
server = app.server

app.title = 'Kraftmessdatenauswerteprogramm'
app.layout = html.Div([
    html.Div([
        dcc.Input(id='lines-header-prim', type='number', value=11),
        dcc.Input(id='columns-read-prim', type='text', value='0,1'),
        dcc.Input(id='line-title', type='number', value=3),
        dcc.Input(id='lines-header-sec', type='number', value=11),
        dcc.Input(id='columns-read-sec', type='text', value='0,1,2'),
        html.Button('Neuer Plot', id='button-1', n_clicks=0),
        html.Button('2. Plot', id='button-2', n_clicks=0),
        html.Button('clear', id='button-3', n_clicks=0),
        html.Button('Dateivorschau', id='button-4', n_clicks=0),
        dcc.Checklist(id='separator-var',
                      options=[{'label': 'Semikolon', 'value': ';'}], value=[';']),
        dcc.RadioItems(id='max-var', options=[{'label': 'Messdaten', 'value': 0},
                                              {'label': 'mit Maxima', 'value': 1},
                                              {'label': 'nur Maxima', 'value': 2}], value=0),
        dcc.Checklist(
            id='time-var', options=[{'label': 'Anzeige nach Uhrzeit', 'value': 1}], value=[0]),
        dcc.Checklist(
            id='point-var', options=[{'label': 'Mit Datenpunkten', 'value': 1}], value=[0]),
        html.Div(id='output')
    ]),
    html.Div([
        dcc.Graph(id='plot-1'),
        dcc.Graph(id='plot-2')
    ])
])


@app.callback(Output('output', 'children'),
              [Input('button-1', 'n_clicks'),
               Input('button-2', 'n_clicks'),
               Input('button-3', 'n_clicks'),
               Input('button-4', 'n_clicks')])
def update_output(btn1, btn2, btn3, btn4):
    # Code to update the output based on button clicks
    pass


if __name__ == '__main__':
    app.run_server(debug=True)
