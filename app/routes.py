from app import app
from flask import render_template, request
import sqlite3
import os

@app.route("/")
def home():
    return render_template("index.html")