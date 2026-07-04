# @saltcorn/mysql-backend

MySQL database backend for [Saltcorn](https://saltcorn.com).

Saltcorn normally runs on PostgreSQL or SQLite. This package is a **pluggable
database driver** that lets it run on **MySQL** instead - you install it as an
npm package and point Saltcorn at it with the `db_driver` config key, so MySQL
support ships without adding to Saltcorn's core dependencies.

> **Requires** a version of Saltcorn core with pluggable-driver support (the
> `SqlDialect` interface in `@saltcorn/db-common` and `db_driver` loading in
> `@saltcorn/data`).

## Install

```bash
npm install @saltcorn/cli          # if not already installed
npm install @saltcorn/mysql-backend
```

## Configure

Create the database first. The connecting user needs `CREATE DATABASE` /
`DROP DATABASE` privileges if you plan to use multi-tenancy (each tenant gets
its own MySQL database):

```sql
CREATE DATABASE saltcorn_mysql_db;
```

Then tell Saltcorn to use the driver, either via the `.saltcorn` config file or
environment variables.

**`.saltcorn` config file** (JSON; `saltcorn setup` writes it, or edit it
directly - it lives at `~/.config/.saltcorn` on Linux,
`~/Library/Preferences/.saltcorn` on macOS):

```json
{
  "db_driver": "@saltcorn/mysql-backend",
  "host": "localhost",
  "port": 3306,
  "user": "root",
  "password": "yourpassword",
  "database": "saltcorn_mysql_db",
  "default_schema": "saltcorn_mysql_db"
}
```

**Environment variables** (equivalent):

```bash
export SALTCORN_DB_DRIVER='@saltcorn/mysql-backend'
export PGHOST='localhost'
export PGPORT='3306'
export PGUSER='root'
export PGPASSWORD='yourpassword'
export PGDATABASE='saltcorn_mysql_db'
export SALTCORN_DEFAULT_SCHEMA='saltcorn_mysql_db'
```

Or as a single connection URL instead of the discrete host/user/... settings:

```bash
export SALTCORN_DB_DRIVER='@saltcorn/mysql-backend'
export DATABASE_URL='mysql://root:yourpassword@localhost:3306/saltcorn_mysql_db'
export SALTCORN_DEFAULT_SCHEMA='saltcorn_mysql_db'
```

`default_schema` **must equal the database name**. MySQL has no separate schema
concept, so this driver maps Saltcorn's Postgres-schema-based tenant model to
"one MySQL database per tenant", using `default_schema` as the tenant-less
"root" database when no tenant is active.

### Connection settings

| Config key | Env var | Meaning |
|---|---|---|
| `db_driver` | `SALTCORN_DB_DRIVER` | Must be `@saltcorn/mysql-backend` to select this driver. |
| `host` | `PGHOST` | DB host (the `PG` prefix is historical - it's read by every driver). |
| `port` | `PGPORT` | DB port (MySQL default `3306`). |
| `user` | `PGUSER` | DB user. |
| `password` | `PGPASSWORD` | DB password. |
| `database` | `PGDATABASE` | Database name. |
| `default_schema` | `SALTCORN_DEFAULT_SCHEMA` | Root namespace - **set equal to `database`** for MySQL. |
| `connectionString` | `DATABASE_URL` | Full `mysql://user:pass@host:port/db` URI. Takes priority over the discrete settings above. |
| `session_secret` | `SALTCORN_SESSION_SECRET` | Session signing secret (optional; a random one is generated with a warning if unset). |
| `multi_tenant` | `SALTCORN_MULTI_TENANT` | `"true"` to enable multi-tenancy. |
| `file_store` | `SALTCORN_FILE_STORE` | Where uploaded files are stored. |

Environment variables are not auto-loaded - `source` them into your shell
before running Saltcorn. When both a `DATABASE_URL` and discrete settings are
present, `DATABASE_URL` wins.

## Usage

Once configured, use the Saltcorn CLI as normal:

```bash
saltcorn reset-schema --force        # create system tables + run migrations
saltcorn serve --port 3000           # start the server
saltcorn create-tenant mytenant      # needs SALTCORN_MULTI_TENANT=true
```

## Multi-tenancy

Each tenant is provisioned as its own MySQL database (the analogue of a
per-tenant Postgres schema). This needs the connecting user to have
`CREATE DATABASE` / `DROP DATABASE` privileges.

## License

MIT
