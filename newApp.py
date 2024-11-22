import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import pandas as pd
import plotly.express as px

app = dash.Dash(__name__)
server = app.server

app.title = 'Kraftmessdatenauswerteprogramm'
# Exemple : Charger ou recevoir des données
def load_data():
    # Exemple de dataframe
    data = pd.DataFrame({
        "Category": ["A", "B", "C", "D"],
        "Values": [10, 20, 30, 40]
    })
    return data

# Créer un layout avec Dash
def create_layout(data):
    fig = px.bar(data, x="Category", y="Values", title="Graphique Exemple")
    return html.Div([
        html.H1("Visualisation avec Dash"),
        dcc.Graph(figure=fig),
        html.Button("Exporter les données", id="export-button"),
        html.Div(id="export-status")
    ])

# Callback pour exporter les données
@app.callback(
    Output("export-status", "children"),
    [Input("export-button", "n_clicks")]
)
def export_data(n_clicks):
    if n_clicks:
        data = load_data()
        data.to_csv("data/exported_data.csv", index=False)
        return "Les données ont été exportées !"
    return ""

# Charger les données et initialiser le layout
data = load_data()
app.layout = create_layout(data)

if __name__ == '__main__':
    app.run_server(debug=True)
