import dash_auth
from env import VALID_USERNAME_PASSWORD_PAIRS


def enable_dash_auth(app):
    auth = dash_auth.BasicAuth(
        app,
        VALID_USERNAME_PASSWORD_PAIRS
    )