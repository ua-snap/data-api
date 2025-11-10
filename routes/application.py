"""Compatibility module to expose the Flask app under routes.application.

Historically some deployment scripts pointed FLASK_APP at ``routes.application``.
To preserve that behaviour we re-export the real application object defined in
the top-level ``application.py`` module.
"""

from application import application as _application, app as _app

# Provide both names so ``routes.application:application`` and ``routes.application:app``
# resolve to the same Flask instance.
application = _application
app = _app

__all__ = ["app", "application"]
