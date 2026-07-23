---
title: dbt
rank: 5
---

# dbt

[`dbt-zelox`](https://github.com/lakehq/dbt-zelox) is the LakeSail-maintained dbt
adapter for Zelox. It is a thin wrapper around `dbt-spark` that connects to Zelox
over Spark Connect, so any existing dbt-spark project runs on Zelox with only a
profile change.

## Installation

```bash
pip install dbt-zelox
```

`dbt-zelox` pulls in `dbt-spark[session]` and `pyzelox` as dependencies. You do
not need to install Zelox separately.

## Configuration

Run `dbt init` to generate a profile interactively. It will prompt for the
fields below.

| Field                    | Required     | Default     | Description                                                                                                            |
| ------------------------ | ------------ | ----------- | ---------------------------------------------------------------------------------------------------------------------- |
| `type`                   | yes          |             | Must be `zelox`.                                                                                                        |
| `mode`                   | yes          | `embedded`  | `embedded` starts a Zelox Spark Connect server in the dbt process. `remote` connects to an already-running Zelox server. |
| `schema`                 | yes          |             | Default schema dbt builds objects in.                                                                                  |
| `host`                   | for `remote` | `127.0.0.1` | Hostname of the Zelox server. In `embedded` mode this is the bind address of the in-process server.                     |
| `port`                   | no           | `50051`     | Port of the Zelox server. In `embedded` mode an unused port is chosen automatically.                                    |
| `database`               | no           | `null`      | Must be omitted or equal to `schema`. Zelox, like Spark, treats database and schema as the same thing.                  |
| `server_side_parameters` | no           | `{}`        | Map of string-valued options forwarded to the Spark Connect session.                                                   |
| `threads`                | no           | `1`         | Standard dbt option.                                                                                                   |

## Links

- [`dbt-zelox` on GitHub](https://github.com/lakehq/dbt-zelox)
- [`dbt-zelox` on PyPI](https://pypi.org/project/dbt-zelox/)
- [dbt documentation](https://docs.getdbt.com/)
- [Getting Started with Zelox](/introduction/getting-started/)
