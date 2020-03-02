from flask import Blueprint, render_template
from flask_cors import CORS

web = Blueprint('web', __name__)
CORS(web)

@web.app_errorhandler(404)
def not_found(e):
    return render_template('404.html'),404

from . import realtime