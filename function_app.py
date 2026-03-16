import azure.functions as func
import logging

from blueprints.debug import debug_bp
from blueprints.ingestion import ingestion_bp
from blueprints.cleaning import cleaning_bp

app = func.FunctionApp()
app.register_blueprint(debug_bp)
app.register_blueprint(ingestion_bp)
app.register_blueprint(cleaning_bp)