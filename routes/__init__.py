from flask import Blueprint

routes = Blueprint("routes", __name__)

from .fire import *
from .permafrost import *
