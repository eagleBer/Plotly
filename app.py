import dash
import dash_html_components as html

# Création de l'application Dash
app = dash.Dash(__name__)

# Définition de la mise en page de l'application
app.layout = html.Div("Hello, world!")

# Point d'entrée de l'application
if __name__ == "__main__":
    app.run_server(debug=True)
