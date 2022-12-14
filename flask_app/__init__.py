from flask import Flask

def create_app(config=None):
    
    app = Flask(__name__)
    
    if config is not None:
        app.config.update(config)
    
    from flask_app.api import api_bp
    from flask_app.views.main_views import main_bp
    
    app.register_blueprint(main_bp)
    app.register_blueprint(api_bp, url_prefix='/api')
    
    return app
        
    