#
# This is the earthmaps.io Varnish configuration file
#
# See the VCL chapters in the Users Guide at https://www.varnish-cache.org/docs/
# and http://varnish-cache.org/trac/wiki/VCLExamples for more examples.

# Marker to tell the VCL compiler that this VCL has been adapted to the
# new 4.0 format.
vcl 4.0;

# Default backend server is the EB instance running the production data-api
backend default {
    .host = "52.13.99.27";
    .port = "80";
    .between_bytes_timeout = 600s;
    .first_byte_timeout = 600s;
}

sub vcl_recv {
    # Happens before we check if we have this in cache already.
    #
    # Typically you clean up the request here, removing cookies you don't need,
    # rewriting the request, etc.

    # If the URL contains /update, never cache it
    if (req.url ~ "^/update($|/.*)") {
        return (pass);
    }

    # If the URL contains /places, never cache it
    if (req.url ~ "^/places/.*") {
        return (pass);
    }
}

sub vcl_backend_response {
    # Happens after we have read the response headers from the backend.
    #
    # Here you clean the response headers, removing silly Set-Cookie headers
    # and other mistakes your backend does.

    # Sets the time-to-live for a cached return to 4 weeks
    set beresp.ttl = 4w;

    # If we ever receive a 500+ error, retry connecting to the backend
    # on the next request to that URL.
    if (beresp.status == 500 || beresp.status == 502 || beresp.status == 503 || beresp.status == 504) {
        return (retry);
    }
}

sub vcl_deliver {
    # Happens when we have all the pieces we need, and are about to send the
    # response to the client.
    #
    # You can do accounting or modifying the final object here.
}
