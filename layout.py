import pandas as pd
import dash
from dash import dcc, html, dash_table, ctx, callback, callback_context
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from dash_bootstrap_components._components.Container import Container
import dash_bootstrap_components as dbc
import io
from io import StringIO, BytesIO
import base64
import string

# Initialiser l'application Dash
app = dash.Dash(__name__)

# Définir la disposition de l'application
app.layout = html.Div([
    html.H1("Calculatrice Numérique"),
    dcc.Tabs(id="tabs", value='tab-1', children=[
        dcc.Tab(label='Calculatrice', value='tab-1', children=[
            dcc.Upload(
                id='upload-data',
                children=html.Div([
                    'Glissez et déposez un fichier ou ',
                    html.A('Sélectionnez un fichier')
                ]),
                style={
                    'width': '50%',
                    'height': '60px',
                    'lineHeight': '60px',
                    'borderWidth': '1px',
                    'borderStyle': 'dashed',
                    'borderRadius': '5px',
                    'textAlign': 'center',
                    'margin': '10px'
                },
                multiple=False
            ),
            dcc.Input(
                id="input-1",
                type="number",
                placeholder="Entrez une valeur",
                min=0,
                max=100,
                required=True,
            ),
            html.Button("Calculer", id="button-1", n_clicks=0),
            html.Div(id="output-1")
        ]),
        dcc.Tab(label='Réinitialisation', value='tab-2', children=[
            html.Button("Réinitialiser", id="reset-button", n_clicks=0),
            html.Div(id="reset-output")
        ])
    ])
])

# Définir la fonction de traitement du fichier téléchargé


def parse_contents(contents, filename):
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        if 'csv' in filename:
            # Lire le fichier CSV
            df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))
        elif 'xls' in filename:
            # Lire le fichier Excel
            df = pd.read_excel(io.BytesIO(decoded))
    except Exception as e:
        print(e)
        return html.Div([
            'Il y a eu une erreur pendant la lecture du fichier.'
        ])
    return df

# Définir la fonction de calcul


def calculate(value):
    if value < 0 or value > 100:
        raise ValueError("La valeur doit être comprise entre 0 et 100.")
    return value * 2

# Définir la fonction de rappel pour la sortie


@app.callback(
    Output("output-1", "children"),
    [Input("button-1", "n_clicks")],
    [State("input-1", "value"),
     State('upload-data', 'contents'),
     State('upload-data', 'filename')]
)
def update_output(n_clicks, value, contents, filename):
    if n_clicks > 0:
        if value is None:
            return "Veuillez entrer une valeur valide."
        else:
            try:
                # Lire le fichier téléchargé
                if contents:
                    df = parse_contents(contents, filename)
                else:
                    df = pd.DataFrame()
                # Calculer le résultat
                result = calculate(value)
                return f"Le résultat est {result}."
            except ValueError as e:
                return str(e)
    else:
        return ""

# Définir la fonction de rappel pour
