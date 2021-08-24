from flask import Blueprint

routes = Blueprint("routes", __name__)

from .fire import *
from .permafrost import *
from .iem import *
from .glacier import *
from .geology import *
from .physiography import *
