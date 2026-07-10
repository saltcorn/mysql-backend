/**
 * MySQL data access layer
 * @category mysql-backend
 * @module mysql
 */
import mysql from "mysql2/promise";
import type { Pool, PoolConnection } from "mysql2/promise";
import { createRequire } from "module";
const require = createRequire(import.meta.url);
import {
  sqlsanitize,
  sqlsanitizeAllowDots,
  mkWhereForDialect,
  mkSelectOptionsForDialect,
  ftsFieldsSqlExpr,
} from "@saltcorn/db-common/internal";
import type {
  Value,
  Where,
  SelectOptions,
  Row,
  SqlDialect,
} from "@saltcorn/db-common/internal";
import tenantsModule from "@saltcorn/db-common/tenants";
import { reprAsJson } from "@saltcorn/db-common/sqlite-commons";

/**
 * mysql2 does not auto-serialize bound parameters into JSON (unlike `pg`,
 * which handles JS objects for jsonb columns itself) - plain objects/arrays
 * need an explicit JSON.stringify before binding. Dates and PlainDate
 * instances are left untouched so mysql2 can bind them as proper
 * DATETIME/DATE values.
 */
const mkVal = (v: any): any => (reprAsJson(v) ? JSON.stringify(v) : v);

const normalizePlaceholders = (
  text: string,
  params?: any[],
): { text: string; params?: any[] } => {
  if (!params || !text.match(/\$\d+/)) return { text, params };
  const orderedParams: any[] = [];
  const normalizedText = text.replace(/\$(\d+)/g, (_match, index) => {
    orderedParams.push(params[+index - 1]);
    return "?";
  });
  return { text: normalizedText, params: orderedParams };
};

let getTenantSchema: () => string;
let getRequestContext: () => any;
let getConnectObject: ((connObj?: any) => any) | null = null;
export let pool: Pool | null = null;

let log_sql_enabled = false;

const quote = (s: string): string => `"${s}"`;

const ppPK = (pk?: string): string => (pk ? quote(pk) : "id");

/*
 * A SqlDialect implementation for MySQL.
 *
 * The mysql2 client uses positional "?" placeholders (like SQLite), and
 * MySQL run with ANSI_QUOTES + PIPES_AS_CONCAT (set once per connection in
 * buildPool() below) accepts double-quoted identifiers and "||" string
 * concatenation exactly like Postgres/SQLite, so the shared
 * quote()/sqlsanitize()/"'%' || ? || '%'"-style machinery in db-common
 * needs no changes.
 */
export const mysqlPlaceHolderStack = (): SqlDialect => {
  let values: Value[] = [];
  const push = (x: Value): string => {
    values.push(x);
    return "?";
  };
  const self: SqlDialect = {
    name: "mysql",
    is_sqlite: false,
    push,
    getValues() {
      return values;
    },
    placeholderAt() {
      return "?";
    },
    like() {
      // MySQL's default collation (*_ci) is already case-insensitive, so a
      // plain LIKE gives ILIKE-equivalent semantics - MySQL has no ILIKE
      // keyword at all.
      return "LIKE";
    },
    regexOperator() {
      return "REGEXP";
    },
    castDateExpr(doCast: boolean, s: string) {
      return !doCast ? s : `DATE(${s})`;
    },
    jsonExtractExpr(fieldExpr: string, jsonPath: string, asText: boolean) {
      return asText
        ? `JSON_UNQUOTE(JSON_EXTRACT(${fieldExpr}, '${jsonPath}'))`
        : `JSON_EXTRACT(${fieldExpr}, '${jsonPath}')`;
    },
    ftsWhereClause(v) {
      // A true MATCH()...AGAINST() implementation needs a FULLTEXT index on
      // the exact column list being searched, but Saltcorn's FTS expression
      // (ftsFieldsSqlExpr) is a computed coalesce/concat across possibly
      // several columns, which MySQL cannot back with a FULLTEXT index (no
      // expression indexes there). Until the (deferred) generic table-DDL
      // work adds MySQL-aware FTS index creation, fall back to LIKE - same
      // degraded-but-correct behaviour already used for the disable_fts case
      // on Postgres and always on SQLite.
      const { fields, table, schema } = v;
      const flds = ftsFieldsSqlExpr(fields, table, schema);
      return `${flds} LIKE '%' || ${push(v.searchTerm)} || '%'`;
    },
    arrayInClause(vals: Value[]) {
      return `IN (${vals.map((v) => push(v)).join(", ")})`;
    },
    slugifyWhereClause(k: string, s: string) {
      return `REGEXP_REPLACE(REPLACE(LOWER(${quote(
        sqlsanitizeAllowDots(k),
      )}),' ','-'),'[^\\w-]','')=${push(s)}`;
    },
    textCastSuffix() {
      return "";
    },
  };
  return self;
};

const mkWhere = (
  whereObj: Where | undefined,
): { where: string; values: Value[] } =>
  mkWhereForDialect(whereObj, mysqlPlaceHolderStack());

const mkSelectOptions = (selopts: SelectOptions, values: any[]): string =>
  mkSelectOptionsForDialect(selopts, values, mysqlPlaceHolderStack());

/**
 * Control Logging sql statements to console
 * @param {boolean} [val = true] - if true then log sql statements to console
 */
export function set_sql_logging(val: boolean = true): void {
  log_sql_enabled = val;
}

/**
 * Get sql logging state
 * @returns {boolean} if true then sql logging eabled
 */
export function get_sql_logging(): boolean {
  return log_sql_enabled;
}

/**
 * Log SQL statement to console
 * @param {string} sql - SQL statement
 * @param {object} [vs] - any additional parameter
 */
export function sql_log(sql: string, vs?: any): void {
  if (log_sql_enabled)
    if (typeof vs === "undefined") console.log(sql);
    else console.log(sql, vs);
}

const buildPool = (connectObj: any): Pool => {
  // DATABASE_URL takes priority over discrete host/user/password/database in
  // connect.ts's getConnectObject() (which deletes the discrete fields when
  // it's set), so a "mysql://user:pass@host:port/db" DATABASE_URL must be
  // handled here too - mysql2's createPool() accepts a plain URI string as
  // an alternative to an options object, same idea as `pg`'s Pool accepting
  // `{ connectionString }`.
  const newPool = connectObj.connectionString
    ? mysql.createPool(
        `${connectObj.connectionString}${
          connectObj.connectionString.includes("?") ? "&" : "?"
        }multipleStatements=true`,
      )
    : mysql.createPool({
        host: connectObj.host,
        port: connectObj.port ? +connectObj.port : undefined,
        user: connectObj.user,
        password: connectObj.password,
        database: connectObj.database || connectObj.default_schema,
        ssl: connectObj.ssl,
        dateStrings: false,
        // migrations and some bootstrap SQL bundle several ";"-separated
        // statements in one query() call, which pg/sqlite3 both tolerate -
        // mysql2 needs this explicitly enabled to match.
        multipleStatements: true,
      });
  // Every query in this codebase double-quotes identifiers (as Postgres and
  // SQLite both do); ANSI_QUOTES makes MySQL accept that same syntax, so
  // none of the "schema"."table" string-building already spread across
  // saltcorn-data needs to change for MySQL. Similarly, "||" is used
  // throughout as the ANSI string-concatenation operator (e.g. the ilike
  // "'%' || ? || '%'" pattern in db-common/internal.ts) - by default MySQL
  // treats "||" as logical OR instead, so PIPES_AS_CONCAT is required too.
  newPool.on("connection", (connection: any) => {
    connection.query(
      "SET SESSION sql_mode = (SELECT CONCAT(@@sql_mode, ',ANSI_QUOTES,PIPES_AS_CONCAT'))",
    );
  });
  return newPool;
};

/**
 * Close database connection
 * @returns {Promise<void>}
 */
export const close = async (): Promise<void> => {
  if (pool) await pool.end();
  pool = null;
};

/**
 * Change connection (close connection and open new connection from connObj)
 * @param {object} [connObj = {}] - connection object
 * @returns {Promise<void>}
 */
export const changeConnection = async (
  connObj: any = Object.create(null),
): Promise<void> => {
  await close();
  pool = buildPool(getConnectObject!(connObj));
};

export const begin = async (): Promise<void> => {
  await query("BEGIN");
};

export const commit = async (): Promise<void> => {
  await query("COMMIT");
};

export const rollback = async (): Promise<void> => {
  await query("ROLLBACK");
};

const getMyClient = (selopts?: any): any => {
  return selopts?.client || getRequestContext()?.client || pool;
};

/**
 * Execute Select statement
 * @param {string} tbl - table name
 * @param {object} whereObj - where object
 * @param {object} [selectopts = {}] - select options
 * @returns {Promise<*>} return rows
 */
export const select = async (
  tbl: string,
  whereObj: Where,
  selectopts: SelectOptions & { [key: string]: any } = Object.create(null),
): Promise<Row[]> => {
  const { where, values } = mkWhere(whereObj);
  const schema = selectopts.schema || getTenantSchema();
  const sql = `SELECT ${
    selectopts.fields ? selectopts.fields.join(", ") : `*`
  } FROM "${schema}"."${sqlsanitize(tbl)}" ${where} ${mkSelectOptions(
    selectopts,
    values,
  )}`;
  sql_log(sql, values);
  const [rows] = await getMyClient(selectopts).query(sql, values);
  return rows as Row[];
};

/**
 * Reset the tenant's database (DROP DATABASE + CREATE DATABASE): the MySQL
 * analogue of Postgres's schema drop/recreate under the one-database-per-
 * tenant model.
 * @param {string} schema - db/tenant name
 * @returns {Promise<void>} no result
 */
export const drop_reset_schema = async (schema: string): Promise<void> => {
  const name = sqlsanitize(schema);
  const dropSql = `DROP DATABASE IF EXISTS "${name}";`;
  const createSql = `CREATE DATABASE "${name}";`;
  sql_log(dropSql);
  await getMyClient().query(dropSql);
  sql_log(createSql);
  await getMyClient().query(createSql);
};

/**
 * Get count of rows in table
 * @param {string} - tbl - table name
 * @param {object} - whereObj - where object
 * @returns {Promise<number>} count of tables
 */
export const count = async (
  tbl: string,
  whereObj: Where,
  opts?: SelectOptions & { [key: string]: any },
): Promise<number> => {
  const { where, values } = mkWhere(whereObj);
  const core_sql = `FROM "${opts?.schema || getTenantSchema()}"."${sqlsanitize(
    tbl,
  )}" ${where}`;
  const sql = opts?.limit
    ? `SELECT count(*) AS count FROM (
  SELECT 1 ${core_sql} limit ${+opts?.limit}) limited_count`
    : `SELECT COUNT(*) AS count ${core_sql}`;
  sql_log(sql, values);
  const [rows]: any = await getMyClient(opts).query(sql, values);
  const countValue = rows[0].count ?? Object.values(rows[0])[0];
  return parseInt(String(countValue));
};

/**
 * Get version of MySQL
 * @param {boolean} short - if true return short version info else full version info
 * @returns {Promise<string>} returns version
 */
export const getVersion = async (short?: boolean): Promise<string> => {
  const sql = `SELECT VERSION() AS version;`;
  sql_log(sql);
  const [rows]: any = await getMyClient().query(sql);
  const v = rows[0].version;
  if (short) return v.split("-")[0];
  return v;
};

/**
 * Delete rows in table
 * @param {string} tbl - table name
 * @param {object} whereObj - where object
 * @param {object} [opts = {}]
 * @returns {Promise<object[]>} result of delete execution
 */
export const deleteWhere = async (
  tbl: string,
  whereObj: Where,
  opts: { schema?: string; client?: any } = Object.create(null),
): Promise<any> => {
  const { where, values } = mkWhere(whereObj);
  const sql = `delete FROM "${opts.schema || getTenantSchema()}"."${sqlsanitize(
    tbl,
  )}" ${where}`;
  sql_log(sql, values);
  const [result] = await getMyClient(opts).query(sql, values);
  return result;
};

export const truncate = async (tbl: string): Promise<any> => {
  const sql = `truncate "${getTenantSchema()}"."${sqlsanitize(tbl)}"`;
  sql_log(sql, []);
  const [result] = await getMyClient().query(sql, []);
  return result;
};

/**
 * Insert rows into table
 * @param {string} tbl - table name
 * @param {object} obj - columns names and data
 * @param {object} [opts = {}] - columns attributes
 * @returns {Promise<any>} returns primary key column value, unless opts.noid
 */
export const insert = async (
  tbl: string,
  obj: Row,
  opts: {
    schema?: string;
    onConflictDoNothing?: boolean;
    noid?: boolean;
    pk_name?: string;
    client?: any;
  } = Object.create(null),
): Promise<any> => {
  const kvs = Object.entries(obj);
  const fnameList = kvs.map(([k, v]) => `"${sqlsanitize(k)}"`).join();
  const schema = opts.schema || getTenantSchema();
  const client = getMyClient(opts);
  var valPosList: string[] = [];
  var valList: any[] = [];
  kvs.forEach(([k, v]: [string, any]) => {
    if (v && v.next_version_by_id) {
      valList.push(v.next_version_by_id);
      valPosList.push(
        `coalesce((select max(\`_version\`) from "${schema}"."${sqlsanitize(
          tbl,
        )}" where "${v.pk_name || "id"}"=?), 0)+1`,
      );
    } else {
      valList.push(mkVal(v));
      valPosList.push("?");
    }
  });
  const insertIgnore = opts.onConflictDoNothing ? "ignore " : "";
  const sql =
    valPosList.length > 0
      ? `insert ${insertIgnore}into "${schema}"."${sqlsanitize(
          tbl,
        )}"(${fnameList}) values(${valPosList.join()})`
      : `insert ${insertIgnore}into "${schema}"."${sqlsanitize(
          tbl,
        )}" () values()`;
  sql_log(sql, valList);
  const [result]: any = await client.query(sql, valList);
  if (opts.noid) return;
  else if (opts.onConflictDoNothing && !result.insertId) return;
  else return result.insertId;
};

/**
 * Update table records
 * @param {string} tbl - table name
 * @param {object} obj - columns names and data
 * @param {number|undefined} id - id of record (primary key column value)
 * @param {object} [opts = {}] - columns attributes
 * @returns {Promise<void>} no result
 */
export const update = async (
  tbl: string,
  obj: Row,
  id: any,
  opts: { schema?: string; pk_name?: string; client?: any } = Object.create(
    null,
  ),
): Promise<void> => {
  const kvs = Object.entries(obj);
  if (kvs.length === 0) return;
  const assigns = kvs.map(([k, v]) => `"${sqlsanitize(k)}"=?`).join();
  let valList = kvs.map(([k, v]) => mkVal(v));
  let whereS;
  if (id && typeof id == "object") {
    const whereStrs: string[] = [];
    Object.keys(id).forEach((k) => {
      valList.push(id[k]);
      whereStrs.push(`"${k}"=?`);
    });
    whereS = whereStrs.join(" and ");
  } else {
    valList.push(id === "undefined" ? obj[opts.pk_name || "id"] : id);
    whereS = `${ppPK(opts.pk_name)}=?`;
  }
  const q = `update "${opts.schema || getTenantSchema()}"."${sqlsanitize(
    tbl,
  )}" set ${assigns} where ${whereS}`;
  sql_log(q, valList);
  await getMyClient(opts).query(q, valList);
};

/**
 * Update table records matching a where clause
 * @param {string} tbl - table name
 * @param {object} obj - columns names and data
 * @param {object} whereObj - where object
 * @param {object} opts - can contain a db client for transactions
 * @returns {Promise<void>} no result
 */
export const updateWhere = async (
  tbl: string,
  obj: Row,
  whereObj: Where,
  opts: { client?: any } = Object.create(null),
): Promise<void> => {
  const kvs = Object.entries(obj);
  if (kvs.length === 0) return;
  const { where, values } = mkWhere(whereObj);
  const assigns = kvs.map(([k, v]) => `"${sqlsanitize(k)}"=?`).join();
  let valList = [...kvs.map(([k, v]) => mkVal(v)), ...values];

  const q = `update "${getTenantSchema()}"."${sqlsanitize(
    tbl,
  )}" set ${assigns} ${where}`;
  sql_log(q, valList);
  await getMyClient(opts).query(q, valList);
};

/**
 * Select one record
 * @param {string} tbl - table name
 * @param {object} where - where object
 * @param {object} [selectopts = {}] - select options
 * @returns {Promise<object>} return first record from sql result
 * @throws {Error}
 */
export const selectOne = async (
  tbl: string,
  where: Where,
  selectopts: SelectOptions = Object.create(null),
): Promise<Row> => {
  const rows = await select(tbl, where, { ...selectopts, limit: 1 });
  if (rows.length === 0) {
    const w = mkWhere(where);
    throw new Error(`no ${tbl} ${w.where} are ${w.values}`);
  } else return rows[0];
};

/**
 * Select one record or null if no records
 * @param {string} tbl - table name
 * @param {object} where - where object
 * @param {object} [selectopts = {}] - select options
 * @returns {Promise<null|object>} - null if no record or first record data
 */
export const selectMaybeOne = async (
  tbl: string,
  where: Where,
  selectopts: SelectOptions = Object.create(null),
): Promise<Row | null> => {
  const rows = await select(tbl, where, { ...selectopts, limit: 1 });
  if (rows.length === 0) return null;
  else return rows[0];
};

/**
 * Open db connection
 * @returns {Promise<*>} db connection object
 */
export const getClient = async (): Promise<PoolConnection> =>
  await pool!.getConnection();

/**
 * Reset auto-increment counter (the MySQL analogue of Postgres's sequence reset)
 * @param {string} tblname - table name
 * @returns {Promise<void>} no result
 */
export const reset_sequence = async (
  tblname: string,
  pkname: string = "id",
): Promise<void> => {
  const schema = getTenantSchema();
  const selSql = `SELECT COALESCE(MAX("${sqlsanitize(
    pkname,
  )}"), 0) + 1 AS next_id FROM "${schema}"."${sqlsanitize(tblname)}"`;
  const [rows]: any = await getMyClient().query(selSql);
  const nextId = rows[0].next_id;
  const alterSql = `ALTER TABLE "${schema}"."${sqlsanitize(
    tblname,
  )}" AUTO_INCREMENT = ${+nextId};`;
  sql_log(alterSql);
  await getMyClient().query(alterSql);
};

/*
 * MySQL cannot index/unique-constrain a TEXT/BLOB column without an explicit
 * key prefix length ("BLOB/TEXT column used in key specification without a
 * key length"). Saltcorn's String type maps to TEXT, so index/unique targets
 * frequently are TEXT columns - look their types up and add a prefix. 191
 * utf8mb4 chars fits the classic 767-byte InnoDB key limit.
 */
const keyColumnExprs = async (
  table_name: string,
  field_names: string[],
): Promise<string> => {
  const [rows]: any = await getMyClient().query(
    `SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
    [getTenantSchema(), table_name],
  );
  const needsPrefix = new Set(
    rows
      .filter((r: any) => /text|blob/i.test(r.DATA_TYPE || r.data_type || ""))
      .map((r: any) => r.COLUMN_NAME || r.column_name),
  );
  return field_names
    .map(
      (f) =>
        `"${sqlsanitize(f)}"${needsPrefix.has(sqlsanitize(f)) ? "(191)" : ""}`,
    )
    .join(",");
};

/**
 * Add unique constraint
 * @param {string} table_name - table name
 * @param {string[]} field_names - list of columns (members of constraint)
 * @returns {Promise<void>} no result
 */
export const add_unique_constraint = async (
  table_name: string,
  field_names: string[],
): Promise<void> => {
  const sql = `alter table "${getTenantSchema()}"."${sqlsanitize(
    table_name,
  )}" add CONSTRAINT "${sqlsanitize(table_name)}_${field_names
    .map((f) => sqlsanitize(f))
    .join("_")}_unique" UNIQUE (${await keyColumnExprs(
    table_name,
    field_names,
  )});`;
  sql_log(sql);
  await getMyClient().query(sql);
};

/**
 * Drop unique constraint
 * @param {string} table_name - table name
 * @param {string[]} field_names - list of columns (members of constraint)
 * @returns {Promise<void>} no results
 */
export const drop_unique_constraint = async (
  table_name: string,
  field_names: string[],
): Promise<void> => {
  const sql = `alter table "${getTenantSchema()}"."${sqlsanitize(
    table_name,
  )}" drop index "${sqlsanitize(table_name)}_${field_names
    .map((f) => sqlsanitize(f))
    .join("_")}_unique";`;
  sql_log(sql);
  try {
    await getMyClient().query(sql);
  } catch (e: any) {
    // MySQL has no "DROP INDEX IF EXISTS" equivalent for this form - swallow
    // "unknown key" errors to match the IF EXISTS semantics postgres.ts has
    if (!/check that column\/key exists|doesn't exist/i.test(e?.message || ""))
      throw e;
  }
};

/**
 * Add index
 * @param {string} table_name - table name
 * @param {string} field_name - column name
 * @returns {Promise<void>} no result
 */
export const add_index = async (
  table_name: string,
  field_name: string,
): Promise<void> => {
  const sql = `create index "${sqlsanitize(table_name)}_${sqlsanitize(
    field_name,
  )}_index" on "${getTenantSchema()}"."${sqlsanitize(
    table_name,
  )}" (${await keyColumnExprs(table_name, [field_name])});`;
  sql_log(sql);
  await getMyClient().query(sql);
};

/**
 * Add Full-text search index
 *
 * Not currently implemented for MySQL - Saltcorn's FTS expression is a
 * computed coalesce/concat across possibly several columns
 * (ftsFieldsSqlExpr), and MySQL FULLTEXT indexes can only be built on real
 * column lists, not arbitrary expressions. Search still works via the LIKE
 * fallback in mysqlPlaceHolderStack().ftsWhereClause - it's just not backed
 * by an index. Revisit once MySQL-aware FTS index creation is added
 * alongside the generic table-DDL work.
 * @returns {Promise<void>} no result
 */
export const add_fts_index = async (
  table_name: string,
  field_expression: string,
  language?: string,
  disable_fts?: boolean,
): Promise<void> => {
  console.warn(
    `add_fts_index: full-text indexing is not yet implemented for MySQL (table ${table_name}) - search will use LIKE without an index`,
  );
};
export const drop_fts_index = async (table_name: string): Promise<void> => {};

/**
 * Drop index
 * @param {string} table_name - table name
 * @param {string} field_name - column name
 * @returns {Promise<void>} no result
 */
export const drop_index = async (
  table_name: string,
  field_name: string,
): Promise<void> => {
  const sql = `drop index "${sqlsanitize(table_name)}_${sqlsanitize(
    field_name,
  )}_index" on "${getTenantSchema()}"."${sqlsanitize(table_name)}";`;
  sql_log(sql);
  await getMyClient().query(sql);
};

/**
 * Bulk-load rows from a CSV stream into a table.
 *
 * MySQL has no streaming COPY equivalent that doesn't require server-side
 * file access (LOAD DATA INFILE needs FILE privilege / secure_file_priv
 * configuration that isn't guaranteed to be available), so this reads the
 * stream and issues batched multi-row INSERTs instead. Slower than
 * Postgres's COPY for very large files, but correct.
 * @param {object} fileStream - file stream (CSV, with header row)
 * @param {string} tableName - table name
 * @param {string[]} fieldNames - list of columns
 * @param {object} client - db connection
 * @returns {Promise<void>} no result
 */
export const copyFrom = async (
  fileStream: any,
  tableName: string,
  fieldNames: string[],
  client: any,
): Promise<any> => {
  const { parse } = require("csv-parse/sync");
  const chunks: Buffer[] = [];
  for await (const chunk of fileStream) chunks.push(chunk);
  const text = Buffer.concat(chunks).toString("utf8");
  const records: string[][] = parse(text, { columns: false, from_line: 2 });
  const schema = getTenantSchema();
  const fnameList = fieldNames.map((f) => `"${sqlsanitize(f)}"`).join();
  const BATCH = 500;
  for (let i = 0; i < records.length; i += BATCH) {
    const batch = records.slice(i, i + BATCH);
    if (batch.length === 0) continue;
    const valPosList = batch
      .map((row) => `(${row.map(() => "?").join(",")})`)
      .join(",");
    const sql = `insert into "${schema}"."${sqlsanitize(
      tableName,
    )}"(${fnameList}) values ${valPosList}`;
    await (client || getMyClient()).query(sql, batch.flat());
  }
};

export const copyToJson = async (
  fileStream: any,
  tableName: string,
  client?: any,
): Promise<any> => {
  const schema = getTenantSchema();
  const sql = `SELECT * FROM "${schema}"."${sqlsanitize(tableName)}"`;
  const [rows]: any = await (client || getMyClient()).query(sql);
  for (const row of rows) fileStream.write(JSON.stringify(row) + ",");
};

export const slugify = (s: string): string =>
  s
    .toLowerCase()
    .replace(/\s+/g, "-")
    .replace(/[^\w-]/g, "");

export const time = async (): Promise<Date> => {
  const [rows]: any = await getMyClient().query("select now() as now");
  return new Date(rows[0].now);
};

export const listTables = async (): Promise<{ name: string }[]> => {
  const [rows]: any = await getMyClient().query(
    `SELECT table_name FROM information_schema.tables WHERE table_schema = ?`,
    [getTenantSchema()],
  );
  return rows.map((row: any) => ({ name: row.table_name || row.TABLE_NAME }));
};

export const listUserDefinedTables = async (): Promise<{ name: string }[]> => {
  const [rows]: any = await getMyClient().query(
    `SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name NOT LIKE '\\_sc\\_%'`,
    [getTenantSchema()],
  );
  return rows.map((row: any) => ({ name: row.table_name || row.TABLE_NAME }));
};

export const listScTables = async (): Promise<{ name: string }[]> => {
  const [rows]: any = await getMyClient().query(
    `SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name LIKE '\\_sc\\_%'`,
    [getTenantSchema()],
  );
  return rows.map((row: any) => ({ name: row.table_name || row.TABLE_NAME }));
};

/* rules of using this:

- no try catch inside unless you rethrow: wouldnt roll back
- no state.refresh_*() inside: other works wouldnt see updates as they are in transactioon
     - you can use state.refresh_*(true) for update on own worker only

Note: unlike Postgres, MySQL's DDL statements are not transactional (they
implicitly commit any open transaction). withTransaction() is still correct
for the DML this driver issues today (select/insert/update/delete); it would
NOT roll back DDL, same caveat that applies to the (deferred) generic
table-DDL work.
*/
export const withTransaction = async (
  f: (rollback: () => Promise<void>) => Promise<any>,
  onError?: (e: Error) => any,
): Promise<any> => {
  const client = await getClient();
  const reqCon = getRequestContext();
  if (reqCon) reqCon.client = client;
  sql_log("BEGIN;");
  await client.query("BEGIN;");
  let aborted = false;
  const rollback = async () => {
    aborted = true;
    sql_log("ROLLBACK;");
    await client.query("ROLLBACK;");
  };
  try {
    const result = await f(rollback);
    if (!aborted) {
      sql_log("COMMIT;");
      await client.query("COMMIT;");
    }
    return result;
  } catch (error) {
    if (!aborted) {
      sql_log("ROLLBACK;");
      await client.query("ROLLBACK;");
    }
    if (onError) return onError(error as Error);
    else throw error;
  } finally {
    if (reqCon) reqCon.client = null;
    client.release();
  }
};

export const commitAndBeginNewTransaction = async (): Promise<void> => {
  const client = await getMyClient();
  sql_log("COMMIT;");
  await client.query("COMMIT;");
  sql_log("BEGIN;");
  await client.query("BEGIN;");
};

export const tryCatchInTransaction = async (
  f: () => Promise<any>,
  onError?: (e: Error) => any,
): Promise<any> => {
  const rndid = Math.floor(Math.random() * 16777215).toString(16);
  const reqCon = getRequestContext();
  if (reqCon?.client) await query(`SAVEPOINT sp${rndid}`);
  try {
    return await f();
  } catch (error) {
    if (reqCon?.client) await query(`ROLLBACK TO SAVEPOINT sp${rndid}`);
    if (onError) return await onError(error as Error);
  } finally {
    if (reqCon?.client) await query(`RELEASE SAVEPOINT sp${rndid}`);
  }
};

/**
 * Should be used for code that is sometimes called from within a withTransaction block
 * and sometimes not.
 * @param {Function} f logic to execute
 * @param {Function} onError error handler
 * @returns
 */
export const openOrUseTransaction = async (
  f: (rollback?: () => Promise<void>) => Promise<any>,
  onError?: (e: Error) => any,
): Promise<any> => {
  const reqCon = getRequestContext();
  if (reqCon?.client) return await f();
  else return await withTransaction(f, onError);
};

/**
 * Wait some time until current transaction COMMITs,
 * then open another transaction.
 * @param {Function} f logic to execute
 * @param {Function} onError error handler
 * @returns
 */
export const whenTransactionisFree = (
  f: (rollback?: () => Promise<void>) => Promise<any>,
  onError?: (e: Error) => any,
): Promise<any> => {
  return new Promise((resolve, reject) => {
    let counter = 0;
    const interval = setInterval(async () => {
      const reqCon = getRequestContext();
      if (!reqCon?.client) {
        clearInterval(interval);
        try {
          resolve(await withTransaction(f, onError));
        } catch (e) {
          reject(e);
        }
      }
      if (++counter > 100) {
        clearInterval(interval);
        reject(new Error("Timeout waiting for transaction to be free"));
      }
    }, 200);
  });
};

export const query = async (text: string, params?: any[]): Promise<any> => {
  const normalized = normalizePlaceholders(text, params);
  sql_log(normalized.text, normalized.params);
  const [rows] = await getMyClient().query(normalized.text, normalized.params);
  return { rows };
};

export { mkWhere };

// Registered by saltcorn-data's db/index.ts as the default dialect for the
// boolean-signature mkWhere/mkSelectOptions, so core code that builds SQL
// itself (getJoinedQuery, aggregation subqueries, ...) emits "?" placeholders
// and LIKE/REGEXP/JSON_EXTRACT etc. instead of assuming Postgres.
export const sqlDialectFactory = (initCount?: number) =>
  mysqlPlaceHolderStack();

export const driverName = "mysql";
export const array_agg_sql_fn = "JSON_ARRAYAGG";
export const serial_pk_sql_type = "INT AUTO_INCREMENT";
export const json_sql_type = "JSON";
export const indexable_text_sql_type = "VARCHAR(255)";
export const supports_search_path = false;

// a `references tbl(col) [on delete ...]` clause, as one regex fragment
const REF = `references\\s+"?\\w+"?\\s*\\([^)]*\\)(?:\\s+on\\s+delete\\s+(?:cascade|set\\s+null|restrict|no\\s+action))?`;

// TEXT columns named in a table-level UNIQUE(...) need a bounded type in
// MySQL. Find the columns each UNIQUE(...) references and bound their `text`
// definitions to varchar. (Numeric columns in the same UNIQUE are left alone.)
const boundTextKeyColumns = (sql: string): string => {
  const cols = new Set<string>();
  for (const m of sql.matchAll(/\bunique\s*\(([^)]*)\)/gi))
    m[1]
      .split(",")
      .forEach((c) => cols.add(c.trim().replace(/"/g, "").toLowerCase()));
  let out = sql;
  for (const col of cols)
    out = out.replace(
      new RegExp(`("?\\b${col}\\b"?\\s+)text\\b`, "gi"),
      "$1varchar(255)",
    );
  return out;
};

/**
 * Translate PostgreSQL migration SQL to MySQL (type names, key/default rules,
 * reserved words, and other dialect differences).
 */
export const translateMigrationsFromPostgresql = (sqlIn: string): string => {
  let sql = sqlIn
    // UNLOGGED is a Postgres-only durability hint
    .replace(/\bunlogged\s+table\b/gi, "table")
    // MySQL has no "IF NOT EXISTS" on CREATE INDEX
    .replace(
      /\bcreate\s+(unique\s+)?index\s+if\s+not\s+exists\s+/gi,
      "create $1index ",
    )
    // type names
    .replace(/\bserial\b/gi, "int auto_increment")
    .replace(/\bjsonb\b/gi, "json")
    .replace(/\btimestamptz\b/gi, "timestamp")
    .replace(/\bbytea\b/gi, "longblob")
    // ALTER COLUMN ... TYPE t [USING ...] -> MySQL MODIFY COLUMN ... t
    .replace(
      /\balter\s+column\s+("?\w+"?)\s+type\s+(\w+(?:\([^)]*\))?)(?:\s+using\s+[^;]+)?/gi,
      "modify column $1 $2",
    )
    // ALTER COLUMN ... SET DEFAULT on a TEXT column is rejected by MySQL; drop
    // the statement (migrations that use it also backfill via UPDATE)
    .replace(
      /alter\s+table\s+[^;]*?\balter\s+column\s+\w+\s+set\s+default\b[^;]*;/gi,
      "",
    )
    // reserved words used as column names
    .replace(/\b(add\s+column\s+|[,(]\s*)(read|stored)\b/gi, '$1"$2"')
    // inline column references must come after NOT NULL / DEFAULT in MySQL
    .replace(new RegExp(`(${REF})\\s+(default\\s+[^\\s,;)]+)`, "gi"), "$2 $1")
    .replace(new RegExp(`(${REF})\\s+(not\\s+null)`, "gi"), "$2 $1")
    // TEXT can't be indexed/keyed without a length. Bound inline "text ...
    // unique/primary key" and the ubiquitous "name text" (frequently keyed).
    .replace(
      /\btext\b((?:\s+not\s+null)?\s+(?:unique|primary\s+key))/gi,
      "varchar(255)$1",
    )
    .replace(/(\bname"?\s+)text\b/gi, "$1varchar(255)")
    // TEXT/JSON/BLOB columns require a parenthesised default expression
    .replace(/\bdefault\s+('(?:[^']|'')*')/gi, "default ($1)");
  return boundTextKeyColumns(sql);
};

/**
 * Create the MySQL database backing a tenant namespace.
 * @param {string} name - tenant/database name
 * @param {boolean} [ifNotExists] - use IF NOT EXISTS (idempotent, used by tests)
 * @returns {Promise<void>} no result
 */
export const create_tenant_schema = async (
  name: string,
  ifNotExists?: boolean,
): Promise<void> => {
  const sql = `CREATE DATABASE ${
    ifNotExists ? "IF NOT EXISTS " : ""
  }"${sqlsanitize(name)}";`;
  sql_log(sql);
  await getMyClient().query(sql);
};

/**
 * Drop the MySQL database backing a tenant namespace.
 * @param {string} name - tenant/database name
 * @returns {Promise<void>} no result
 */
export const drop_tenant_schema = async (name: string): Promise<void> => {
  const sql = `DROP DATABASE IF EXISTS "${sqlsanitize(name)}";`;
  sql_log(sql);
  await getMyClient().query(sql);
};

/**
 * Upsert a row into _sc_config.
 * @param {string} key
 * @param {any} value
 * @returns {Promise<void>} no result
 */
export const upsert_config = async (key: string, value: any): Promise<void> => {
  const schema = getTenantSchema();
  const jsonVal = JSON.stringify({ v: value });
  const sql = `insert into "${schema}"."_sc_config"("key", value) values(?, ?)
                on duplicate key update value = ?`;
  sql_log(sql, [key, jsonVal, jsonVal]);
  await getMyClient().query(sql, [key, jsonVal, jsonVal]);
};

/**
 * Build the express-session Store backed by this MySQL pool.
 * @param {any} session - the express-session module instance the app uses
 * @param {object} [opts]
 * @returns {any} a session.Store instance
 */
export const getExpressSessionStore = (
  session: any,
  opts: { pruneInterval?: number } = {},
): any => {
  const MySQLStore = require("express-mysql-session")(session);
  return new MySQLStore(
    {
      schema: {
        tableName: "_sc_session",
        columnNames: {
          session_id: "sid",
          expires: "expire",
          data: "sess",
        },
      },
      createDatabaseTable: true,
      clearExpired: (opts.pruneInterval ?? 0) > 0,
      checkExpirationInterval: (opts.pruneInterval ?? 0) * 1000 || undefined,
    },
    pool,
  );
};

/**
 * Initializes internals of the mysql module.
 * It must be called after importing the module.
 * @param getConnectObjectPara function returning the connection object
 */
export const init = (getConnectObjectPara: (connObj?: any) => any): void => {
  if (!pool) {
    getConnectObject = getConnectObjectPara;
    const connectObj = getConnectObject();
    if (connectObj) {
      pool = buildPool(connectObj);
      getTenantSchema = tenantsModule(connectObj).getTenantSchema;
      getRequestContext = tenantsModule(connectObj).getRequestContext;
    } else {
      throw new Error("Unable to retrieve a database connection object.");
    }
  }
};
