import dash
from dash import Dash, dcc, html
import dash_bootstrap_components as dbc
import logging
from auth import enable_dash_auth
import os

# ************** Init **************

# *** logger
WRITE_LOG_TO_FILE = False
LOG_FORMAT = '%(name)s (%(levelname)s) %(asctime)s: %(message)s'
LOG_LEVEL = logging.INFO

logger = logging.getLogger('main')

if WRITE_LOG_TO_FILE:
    logging.basicConfig(filename='log_dash.txt', filemode='w', format=LOG_FORMAT, level=LOG_LEVEL,
                        datefmt='%d/%m/%y %H:%M:%S')
else:
    logging.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL, datefmt='%d/%m/%y %H:%M:%S')

logging.getLogger('werkzeug').setLevel(logging.WARNING)

# *** app

app = Dash(__name__, title='Планы продаж', external_stylesheets=[dbc.themes.MINTY],
           meta_tags=[{"name": "viewport", 'content': 'width=device-width, initial-scale=1.0'}],
           url_base_pathname='/sales_program/', use_pages=True)
enable_dash_auth(app)


# ************** Layout **************
# logo_img = 'img/logo.jpg'  # replace with your own image

navbar = dbc.NavbarSimple(
    className="navbar",
    children=[
        html.Div('Версия прогноза', style={'color': 'white'}),
        dcc.Dropdown(id="db", clearable=False, persistence=True, persistence_type='session', options=[0, 1, 2],
                     value=0),
        *[dbc.NavItem(dbc.NavLink(page['title'], href=page["relative_path"]))
          for page in dash.page_registry.values()],
    ],
    brand="Планирование продаж",
    brand_href="#",
    color="primary",
    dark=True,
)

app.layout = html.Div([
    html.Div(
        [
            navbar,
        ]
    ),

    dash.page_container
])

if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0')
