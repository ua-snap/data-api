from flask import Blueprint

routes = Blueprint("routes", __name__)

from .fire import *
from .permafrost import *
from .taspr import *
from .glacier import *
from .geology import *
from .physiography import *
from .forest import *
from .mean_annual_precip import *
from .boundary import *
from .vectordata import *