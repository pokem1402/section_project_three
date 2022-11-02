from flask import Flask

def create_app(config=None):
    
    app = Flask(__name__)
    
    if config is not None:
        app.config.update(config)
    
    from flask_app.api import api_bp
    
    app.register_blueprint(api_bp, url_prefix='/api')
    
    
    return app
        
    