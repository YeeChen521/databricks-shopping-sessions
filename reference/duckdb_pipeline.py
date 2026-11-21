#!/usr/bin/env python3
"""
Shopping Sessions: DuckDB reference implementation.

Correctness baseline used to cross-check the native C++ pipeline on fresh seeds.
Reads $DATA_DIR, writes $OUTPUT_DIR/{user_daily,campaign_daily}/part-0.parquet.
Optional positional args: DATA_DIR OUTPUT_DIR (as passed by run_fixtures.py).
"""
import json
import os
import re
import sys
import time

import duckdb

args = [a for a in sys.argv[1:]]
DATA = args[0] if len(args) >= 2 else os.environ.get("DATA_DIR", "/data")
OUT = args[1] if len(args) >= 2 else os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))
VERBOSE = os.environ.get("VERBOSE") == "1"

T0 = time.time()


def log(msg):
    if VERBOSE:
        print(f"[{time.time() - T0:7.2f}s] {msg}", file=sys.stderr)


con = duckdb.connect()
con.execute(f"SET threads = {NCPU}")
con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.8))}GB'")
if os.path.isdir(SCRATCH):
    con.execute(f"SET temp_directory = '{os.path.join(SCRATCH, 'duckdb_tmp')}'")
con.execute("SET preserve_insertion_order = false")


def f1_to_ms(s):
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z", s)
    import calendar
    y, mo, d, h, mi, se = map(int, m.groups())
    return calendar.timegm((y, mo, d, h, mi, se)) * 1000


with open(os.path.join(DATA, "manifest.json")) as f:
    manifest = json.load(f)
WS = f1_to_ms(manifest["window_start"])
WE = f1_to_ms(manifest["window_end"])
DAY = 86_400_000
log(f"window {WS} .. {WE}")


def q(path):
    return "'" + path.replace("'", "''") + "'"


# ---------------------------------------------------------------------------
# Macros: CSV field handling and timestamp parsing, exactly per rules.md.
# ---------------------------------------------------------------------------
con.execute(r"""
CREATE MACRO field_ok(x) AS regexp_full_match(x, '"[^"]*"|[^"]*');
CREATE MACRO field_val(x) AS nullif(CASE WHEN starts_with(x, '"') THEN substr(x, 2, length(x) - 2) ELSE x END, '');
CREATE MACRO is_leap(y) AS (y % 4 = 0 AND (y % 100 <> 0 OR y % 400 = 0));
CREATE MACRO dim(y, m) AS CASE WHEN m = 2 THEN CASE WHEN is_leap(y) THEN 29 ELSE 28 END
                               WHEN m IN (4, 6, 9, 11) THEN 30 ELSE 31 END;
""")

# days from civil (Howard Hinnant), integer arithmetic only, valid for years >= 1.
con.execute(r"""
CREATE MACRO yy(y, m) AS (y - CASE WHEN m <= 2 THEN 1 ELSE 0 END);
CREATE MACRO days_civil(y, m, d) AS (
    (yy(y, m) // 400) * 146097
    + (yy(y, m) - (yy(y, m) // 400) * 400) * 365
    + (yy(y, m) - (yy(y, m) // 400) * 400) // 4
    - (yy(y, m) - (yy(y, m) // 400) * 400) // 100
    + (153 * ((m + 9) % 12) + 2) // 5 + d - 1
    - 719468
);
-- Returns epoch milliseconds as HUGEINT, or NULL for a bad timestamp (window check is separate).
CREATE MACRO parse_ts(s) AS CASE
    WHEN s IS NULL THEN NULL
    WHEN regexp_full_match(s, '[0-9]+') THEN
        CASE WHEN length(ltrim(s, '0')) > 20 THEN NULL
             ELSE TRY_CAST(CASE WHEN ltrim(s, '0') = '' THEN '0' ELSE ltrim(s, '0') END AS HUGEINT) END
    WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-][0-9]{2}:[0-9]{2})') THEN
        CASE WHEN
            CAST(substr(s, 6, 2) AS INT) BETWEEN 1 AND 12
            AND CAST(substr(s, 9, 2) AS INT) >= 1
            AND CAST(substr(s, 9, 2) AS INT) <= dim(CAST(substr(s, 1, 4) AS INT), CAST(substr(s, 6, 2) AS INT))
            AND CAST(substr(s, 12, 2) AS INT) <= 23
            AND CAST(substr(s, 15, 2) AS INT) <= 59
            AND CAST(substr(s, 18, 2) AS INT) <= 59
            AND (length(s) = 20 OR (CAST(substr(s, 21, 2) AS INT) <= 23 AND CAST(substr(s, 24, 2) AS INT) <= 59))
        THEN
            CAST(days_civil(CAST(substr(s, 1, 4) AS BIGINT), CAST(substr(s, 6, 2) AS BIGINT), CAST(substr(s, 9, 2) AS BIGINT)) AS HUGEINT) * 86400000
            + CAST(substr(s, 12, 2) AS BIGINT) * 3600000
            + CAST(substr(s, 15, 2) AS BIGINT) * 60000
            + CAST(substr(s, 18, 2) AS BIGINT) * 1000
            - CASE WHEN length(s) = 20 THEN 0
                   ELSE (CASE WHEN substr(s, 20, 1) = '-' THEN -1 ELSE 1 END)
                        * (CAST(substr(s, 21, 2) AS BIGINT) * 60 + CAST(substr(s, 24, 2) AS BIGINT)) * 60000
              END
        ELSE NULL END
    ELSE NULL
END;
""")


def lines_table(name, folder, nfields):
    """Parse every line (except each file's first) of folder/*.csv into a table of string columns f1..fn."""
    glob = os.path.join(DATA, folder, "*.csv")
    cols = ", ".join(f"field_val(p[{i + 1}]) AS f{i + 1}" for i in range(nfields))
    con.execute(f"""
        CREATE OR REPLACE TABLE {name} AS
        WITH files AS (
            SELECT string_split(content, chr(10)) AS parts FROM read_text({q(glob)})
        ),
        lines AS (
            SELECT unnest(parts) AS line, generate_subscripts(parts, 1) AS idx FROM files
        ),
        split AS (
            SELECT string_split(line, ',') AS p FROM lines WHERE idx > 1 AND line <> ''
        )
        SELECT {cols} FROM split
        WHERE len(p) = {nfields} AND list_bool_and(list_transform(p, lambda x: field_ok(x)))
    """)


def has_files(folder):
    d = os.path.join(DATA, folder)
    return os.path.isdir(d) and any(n.endswith(".csv") for n in os.listdir(d))


def lines_or_empty(name, folder, nfields):
    if has_files(folder):
        lines_table(name, folder, nfields)
    else:
        cols = ", ".join(f"NULL::VARCHAR AS f{i + 1}" for i in range(nfields))
        con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT {cols} WHERE false")


# ---------------------------------------------------------------------------
# Lookups
# ---------------------------------------------------------------------------
def lookup(name, file, nfields=2):
    path = os.path.join(DATA, file)
    cols = ", ".join(f"field_val(p[{i + 1}]) AS f{i + 1}" for i in range(nfields))
    con.execute(f"""
        CREATE OR REPLACE TABLE {name} AS
        WITH lines AS (
            SELECT unnest(parts) AS line, generate_subscripts(parts, 1) AS idx
            FROM (SELECT string_split(content, chr(10)) AS parts FROM read_text({q(path)}))
        ),
        split AS (SELECT string_split(line, ',') AS p FROM lines WHERE idx > 1 AND line <> '')
        SELECT {cols} FROM split WHERE len(p) = {nfields} AND list_bool_and(list_transform(p, lambda x: field_ok(x)))
    """)


lookup("products_raw", "products.csv")
con.execute("CREATE OR REPLACE TABLE products AS SELECT f1 AS product_id, any_value(f2) AS category FROM products_raw WHERE f1 IS NOT NULL GROUP BY f1")
lookup("campaigns_raw", "campaigns.csv")
con.execute("CREATE OR REPLACE TABLE campaigns AS SELECT f1 AS campaign_id, any_value(f2) AS channel FROM campaigns_raw WHERE f1 IS NOT NULL GROUP BY f1")
log("lookups")

# ---------------------------------------------------------------------------
# Links -> persons (connected components, named by the smallest ID)
# ---------------------------------------------------------------------------
lines_or_empty("links_raw", "links", 2)
con.execute("""
    CREATE OR REPLACE TABLE edges AS
    SELECT DISTINCT a, b FROM (
        SELECT f1 AS a, f2 AS b FROM links_raw WHERE f1 IS NOT NULL AND f2 IS NOT NULL
        UNION ALL
        SELECT f2, f1 FROM links_raw WHERE f1 IS NOT NULL AND f2 IS NOT NULL
    ) WHERE a <> b
""")
con.execute("""
    CREATE OR REPLACE TABLE label AS
    SELECT id, id AS lab FROM (
        SELECT f1 AS id FROM links_raw WHERE f1 IS NOT NULL AND f2 IS NOT NULL
        UNION SELECT f2 FROM links_raw WHERE f1 IS NOT NULL AND f2 IS NOT NULL)
""")
it = 0
while True:
    it += 1
    # min over neighbours, then pointer jumping
    con.execute("""
        CREATE OR REPLACE TABLE label2 AS
        WITH nb AS (
            SELECT e.a AS id, min(l.lab) AS m FROM edges e JOIN label l ON l.id = e.b GROUP BY e.a
        ),
        step AS (
            SELECT l.id, least(l.lab, coalesce(nb.m, l.lab)) AS lab FROM label l LEFT JOIN nb ON nb.id = l.id
        )
        SELECT s.id, least(s.lab, coalesce(j.lab, s.lab)) AS lab FROM step s LEFT JOIN step j ON j.id = s.lab
    """)
    changed = con.execute("SELECT count(*) FROM label l JOIN label2 m USING (id) WHERE l.lab <> m.lab").fetchone()[0]
    con.execute("DROP TABLE label; ALTER TABLE label2 RENAME TO label")
    if changed == 0:
        break
log(f"links: {it} iterations")

# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------
lines_or_empty("ev_raw", "events", 7)
log("events parsed")
con.execute(f"""
    CREATE OR REPLACE TABLE ev AS
    WITH kept AS (
        SELECT f1 AS eid, arg_min(struct_pack(uid := f2, ts := f3, ing := f4, typ := f5, pid := f6, price := f7), f4) AS r
        FROM ev_raw WHERE f1 IS NOT NULL GROUP BY f1
    ),
    v AS (
        SELECT eid, r.uid AS uid, parse_ts(r.ts) AS ts, parse_ts(r.ing) AS ing, r.typ AS typ, r.pid AS pid, r.price AS price
        FROM kept
    ),
    valid AS (
        SELECT uid, CAST(ts AS BIGINT) AS ts, typ, pid,
               CASE WHEN typ = 'purchase' THEN CAST(ltrim(price, '0') AS BIGINT) ELSE 0 END AS price
        FROM v
        WHERE uid IS NOT NULL AND ts IS NOT NULL AND ts >= {WS} AND ts < {WE}
          AND typ IN ('view', 'cart', 'purchase') AND pid IS NOT NULL
          AND (typ <> 'purchase' OR (
                regexp_full_match(price, '[0-9]+') AND length(ltrim(price, '0')) BETWEEN 1 AND 8
                AND CAST(ltrim(price, '0') AS BIGINT) BETWEEN 1 AND 10000000))
          AND NOT (ing - ts > 86400000)
    )
    SELECT coalesce(l.lab, v.uid) AS person, v.ts, v.typ = 'purchase' AS is_purchase, v.price,
           coalesce(p.category, 'unknown') AS category
    FROM valid v LEFT JOIN label l ON l.id = v.uid LEFT JOIN products p ON p.product_id = v.pid
""")
log("events validated")

con.execute(f"""
    CREATE OR REPLACE TABLE ev_s AS
    WITH g AS (
        SELECT *, CASE WHEN ts - lag(ts) OVER (PARTITION BY person ORDER BY ts) > 1800000 THEN 1 ELSE 0 END AS brk
        FROM ev
    )
    SELECT *, sum(brk) OVER (PARTITION BY person ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sid
    FROM g
""")
con.execute(f"""
    CREATE OR REPLACE TABLE sessions AS
    SELECT person, sid, min(ts) AS start, CAST(min(ts) // {DAY} AS INT) AS dayn,
           count(*) AS events, sum(price) AS revenue, bool_or(is_purchase) AS purchasing
    FROM ev_s GROUP BY person, sid
""")
log("sessions")

# ---------------------------------------------------------------------------
# Clicks
# ---------------------------------------------------------------------------
lines_or_empty("cl_raw", "clicks", 4)
con.execute(f"""
    CREATE OR REPLACE TABLE clicks AS
    WITH d AS (
        SELECT f1 AS click_id, any_value(struct_pack(uid := f2, cid := f3, ts := f4)) AS r
        FROM cl_raw WHERE f1 IS NOT NULL GROUP BY f1
    ),
    v AS (SELECT click_id, r.uid AS uid, r.cid AS cid, parse_ts(r.ts) AS ts FROM d)
    SELECT coalesce(l.lab, v.uid) AS person, v.click_id, v.cid AS campaign_id, CAST(v.ts AS BIGINT) AS ts
    FROM v JOIN campaigns c ON c.campaign_id = v.cid
    LEFT JOIN label l ON l.id = v.uid
    WHERE v.uid IS NOT NULL AND v.ts IS NOT NULL AND v.ts >= {WS - 7 * DAY} AND v.ts < {WE}
""")
log("clicks")

con.execute(f"""
    CREATE OR REPLACE TABLE cand AS
    SELECT s.person, s.sid, c.click_id, c.campaign_id, c.ts
    FROM sessions s JOIN clicks c ON c.person = s.person AND c.ts <= s.start AND c.ts >= s.start - {7 * DAY}
""")
con.execute("""
    CREATE OR REPLACE TABLE credit AS
    SELECT person, sid, campaign_id FROM cand
    QUALIFY row_number() OVER (PARTITION BY person, sid ORDER BY ts DESC, click_id ASC) = 1
""")
log("attribution")

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------
os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)

con.execute(f"""
    COPY (
        WITH ud AS (
            SELECT person, dayn, count(*) AS sessions, sum(events) AS events, sum(revenue) AS revenue_cents
            FROM sessions GROUP BY person, dayn
        ),
        cat AS (
            SELECT e.person, s.dayn, e.category, sum(e.price) AS rev
            FROM ev_s e JOIN sessions s USING (person, sid)
            WHERE e.is_purchase GROUP BY e.person, s.dayn, e.category
        ),
        top AS (
            SELECT person, dayn, category FROM cat
            QUALIFY row_number() OVER (PARTITION BY person, dayn ORDER BY rev DESC, category ASC) = 1
        )
        SELECT ud.person AS user_id, CAST(DATE '1970-01-01' + ud.dayn AS DATE) AS day,
               CAST(ud.sessions AS BIGINT) AS sessions, CAST(ud.events AS BIGINT) AS events, CAST(ud.revenue_cents AS BIGINT) AS revenue_cents, top.category AS top_category
        FROM ud LEFT JOIN top USING (person, dayn)
    ) TO {q(os.path.join(OUT, 'user_daily', 'part-0.parquet'))} (FORMAT parquet)
""")

con.execute(f"""
    COPY (
        WITH cr AS (
            SELECT coalesce(c.campaign_id, 'organic') AS campaign_id, s.dayn,
                   count(*) AS sessions, count(*) FILTER (WHERE s.purchasing) AS purchasing_sessions,
                   sum(s.revenue) AS revenue_cents
            FROM sessions s LEFT JOIN credit c USING (person, sid)
            GROUP BY ALL
        ),
        asst AS (
            SELECT campaign_id, s.dayn, count(DISTINCT (person, sid)) AS assisted_sessions
            FROM cand JOIN sessions s USING (person, sid)
            GROUP BY ALL
        ),
        k AS (SELECT campaign_id, dayn FROM cr UNION SELECT campaign_id, dayn FROM asst)
        SELECT k.campaign_id, coalesce(cp.channel, 'organic') AS channel,
               CAST(DATE '1970-01-01' + k.dayn AS DATE) AS day,
               CAST(coalesce(cr.sessions, 0) AS BIGINT) AS sessions, CAST(coalesce(cr.purchasing_sessions, 0) AS BIGINT) AS purchasing_sessions,
               CAST(coalesce(cr.revenue_cents, 0) AS BIGINT) AS revenue_cents, CAST(coalesce(asst.assisted_sessions, 0) AS BIGINT) AS assisted_sessions
        FROM k LEFT JOIN cr USING (campaign_id, dayn) LEFT JOIN asst USING (campaign_id, dayn)
        LEFT JOIN campaigns cp ON cp.campaign_id = k.campaign_id
    ) TO {q(os.path.join(OUT, 'campaign_daily', 'part-0.parquet'))} (FORMAT parquet)
""")
log("done")
