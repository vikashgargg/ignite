---
title: Iceberg REST
rank: 2
---

# Iceberg REST Catalog

The Iceberg REST catalog provider in Zelox allows you to connect to an external catalog that exposes the [Iceberg REST Catalog API](https://iceberg.apache.org/rest-catalog-spec/).

An Iceberg REST catalog can be configured using the following options:

- `type` (required): The string `iceberg-rest`.
- `name` (required): The name of the catalog.
- `uri` (required): The base URI of the Iceberg REST catalog server.
- `warehouse` (optional): The warehouse location for the catalog.
- `prefix` (optional): The prefix for all catalog API endpoints.
- `oauth_access_token` (optional): The OAuth 2.0 access token.
- `bearer_access_token` (optional): The bearer token for authentication.

See [Common Options](./index.md#common-options) for caching configuration.

## Examples

```bash
export ZELOX_CATALOG__LIST='[{type="iceberg-rest", name="zelox", uri="https://catalog.example.com"}]'

# OAuth authentication
export ZELOX_CATALOG__LIST='[{type="iceberg-rest", name="zelox", uri="https://catalog.example.com", warehouse="s3://data/warehouse", oauth_access_token="..."}]'

# Bearer token authentication
export ZELOX_CATALOG__LIST='[{type="iceberg-rest", name="zelox", uri="https://catalog.example.com", warehouse="s3://data/warehouse", bearer_access_token="..."}]'
```
