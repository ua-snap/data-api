from flask import Blueprint

routes = Blueprint("routes", __name__)

from .fire import *
from .permafrost import *
from .huc import *
from .iem import *
from .glacier import *
from .geology import *
from .physiography import *
from .forest import *
