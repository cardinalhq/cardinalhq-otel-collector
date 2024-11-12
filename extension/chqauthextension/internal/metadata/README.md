# chqauthextension

This extension is used mostly for the Cardinal HQ SaaS solution, where we
use an API key authentication mechanism with some additional metadata.  While
its use outside that context is possible, its configuration and abilities are
not guaranteed outside the Cardinal use case.

## Client Auth

Client authentication is configured via an API key and some optional environmental
settings to pass additional context to the server.  This extension otherwise
acts as a standard authentication extension coupled with a header setting
extension.

### Options

* `api_key`: a string representing the API key.
* `insecure`: ignore TLS certificate errors.
* `environment`: a `map[string]string` that allows setting arbitrary key/value pairs for use with the Cardinal SaaS service and other Cardinal endpoints.  The only required one for most of the Cardinal API calls is `collector_id=name` to indicate which specific collector group is making the API call.

## Server Auth

Server authentication is made by calling an endpoint to validate and obtain information based on the incoming API key and environment.
There is a cache which is always configured, and defaults to 10 minutes.
The API calls made are documented in `serverauth.go`.

## Examples

```yaml
extensions:
  chqauth:
    client_auth:
      api_key: "${env:CARDINALHQ_API_KEY}"
      environment:
        collector_id: "aws-test-us-east-2"
        customer_id: "e7b8f8e2-4b2d-4f8b-8a1e-1d4f8e2b4b2d"
```
