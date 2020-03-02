from flask import Flask




def create_app():
    app = Flask(__name__)
    app.config.from_object("api.setting")
    register_blueprint(app)
    return app


def register_blueprint(app):
    from api.web import web
    app.register_blueprint(web)