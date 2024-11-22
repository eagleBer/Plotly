from dash import Dash, html, dcc
import plotly.express as px

app = Dash(__name__)

# Exemple d'une figure Plotly
fig = px.scatter(
    x=[1, 2, 3, 4],
    y=[10, 11, 12, 13],
    labels={'x': 'X-Axis', 'y': 'Y-Axis'}
)

app.layout = html.Div([
    html.H1("Hello Dash!"),
    dcc.Graph(figure=fig)
])

if __name__ == '__main__':
    app.run_server(debug=True)
