from flask import Flask

# Initialise Flask app
app = Flask(__name__)

# Import app routes from routes.py
from app import routes