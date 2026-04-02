# chqauthextension

This extension is used mostly for the Cardinal HQ SaaS solution, where we
use an API key authentication mechanism with some additional metadata.  While
its use outside that context is possible, its configuration and abilities are
not guaranteed outside the Cardinal use case.

## Client Auth

Client authentication is configured via an API key.  This extension otherwise
acts as a standard authentication extension coupled with a header setting
extension.

### Options

* `api_key`: a string representing the API key.
* `insecure`: ignore TLS certificate errors.

## Server Auth

Server authentication is made by calling an endpoint to validate and obtain information based on the incoming API key.
There is a cache which is always configured, and defaults to 10 minutes.
The API calls made are documented in `serverauth.go`.

## Examples

```yaml
extensions:
  chqauth:
    client_auth:
      api_key: "${env:CARDINALHQ_API_KEY}"
```
