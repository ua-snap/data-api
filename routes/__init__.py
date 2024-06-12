import os
from flask import Blueprint, redirect
from config import SITE_OFFLINE

routes = Blueprint("routes", __name__)


def check_site_offline():
    if SITE_OFFLINE:
        return redirect("/")


# Applies a decorator to all routes to check for the SITE_OFFLINE environment variable.
@routes.before_request
def enforce_site_offline():
    return check_site_offline()


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
from .hydrology import *
from .demographics import *
from .species import *
