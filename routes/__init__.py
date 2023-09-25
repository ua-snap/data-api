from flask import Blueprint

routes = Blueprint("routes", __name__)

from .fire import *
from .permafrost import *
from .seaice import *
from .taspr import *
from .geology import *
from .physiography import *
from .boundary import *
from .vectordata import *
from .recache import *
from .elevation import *
from .alfresco import *
from .degree_days import *
from .snow import *
from .landfastice import *
from .beetles import *
from .eds import *
from .wet_days_per_year import *
from .indicators import *
