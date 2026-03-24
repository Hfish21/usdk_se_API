# How I Approached This

Before diving into the code, I want to walk through how I actually thought about this problem — the decisions I made, why I made them, and how I used AI tooling to get there faster without letting it drive the car.

---

## Reading the spec

The first thing I did was sit with the PDF for a bit before touching anything. A few things jumped out immediately:

The project has a pretty natural build order — you can't query data that isn't in the database, and you can't ingest data without a schema, and you can't define a schema without knowing what the data looks like. So rather than just starting to write code, I mapped out the dependency chain first:

```
Infrastructure → Data Models → Ingestion → API Endpoints → Notebook → Docs
```

That became the branching strategy. Each step is its own feature branch, merged cleanly into master, so the git history actually tells the story of how the system was built.

One thing I was deliberate about early on: the ingestion should be a one-time CLI script, not an API endpoint. The spec didn't say that explicitly, but it makes more sense operationally — you load your data once (or on a schedule), you don't expose it as a live endpoint. Small decision, but worth making consciously.

---

## The architectural calls

These are the decisions I made before writing any code, and the reasoning behind them.

**Pre-computing `day_of_week` and `period_id` at ingest time**

The speed dataset turned out to have 1.2 million rows. Every API call filters by day and time period. If I computed those at query time — `EXTRACT(DOW FROM timestamp)` on every row, every request — that's a lot of unnecessary work. Instead, I derive them once during ingestion and store them as plain integers. That unlocks a composite B-tree index on `(link_id, day_of_week, period_id)`, so the database can jump straight to the rows it needs rather than scanning and filtering.

**Letting PostGIS handle the GeoJSON serialization**

There's a temptation to pull geometry out of the database as WKB bytes and deserialize it in Python with Shapely. I didn't do that. PostGIS has `ST_AsGeoJSON()` built in, it's fast, and it produces exactly what the API needs to return. Doing that work in Python would just add latency for no gain. So the query returns GeoJSON strings directly, and a Pydantic validator parses them on the way out.

**Hybrid ORM + raw SQL**

I used SQLAlchemy ORM for schema definition — model classes, `create_all()`, the relationship between `Link` and `SpeedRecord`. But for the actual queries, I dropped down to `text()`. The reason is pretty simple: when you've got `GROUP BY`, spatial functions, and CTEs in the same query, the SQLAlchemy ORM expression API gets unwieldy fast. Raw SQL is easier to read, easier to debug, and easier for someone else to pick up. The ORM owns the schema, SQL owns the queries.

**Docker healthcheck**

This one bit me in practice. The `postgis/postgis` container reports as running before it's actually ready to accept connections. Without `depends_on: condition: service_healthy` tied to a `pg_isready` check, the API container would start, try to connect, fail, and crash. The healthcheck makes startup reliable instead of a race condition.

---

## How I used Claude Code

Once the plan was locked, I used Claude Code to implement it. The way I think about this: I'm the architect, Claude Code is a very fast contractor who needs clear specs to do good work.

For each branch, the pattern was:
- Give it the exact requirements for that component
- Review the output before committing anything
- Correct course where needed

This is pretty different from just asking an AI to "build me a geospatial API." If you do that, you get something that technically runs but reflects the AI's assumptions about what you want, not actual engineering decisions. The quality of what comes out is directly proportional to how clearly you can specify what goes in — and that clarity comes from understanding the problem well enough to decompose it.

**Where I had to course-correct on the actual data**

Two things in the real datasets didn't match the spec, and handling them correctly mattered.

The first was geometry type. The spec said LINESTRING, so I modeled it that way. When ingestion ran against the real `link_info` data, the geometries came back as MULTILINESTRING — which makes sense, because real road network data often has multi-part geometries for things like divided highways and complex interchanges. A strict LINESTRING column type caused a PostGIS error. I changed the model to generic GEOMETRY, which is actually the more correct choice for this kind of data regardless.

The second was the geometry format in the parquet file. The `link_info` dataset stores geometry as a GeoJSON string in a column called `geo_json` — not as a proper GeoParquet binary column. `geopandas.read_parquet()` doesn't know what to do with that, so it fails. The fix was to read it with regular pandas, detect the geometry encoding at runtime (WKB bytes, WKT string, or GeoJSON string), and parse accordingly. That kind of format-agnostic handling is pretty normal in geospatial pipelines where data comes from all kinds of sources with inconsistent conventions.

---

## Verifying it actually works

After ingestion finished I checked the row counts, then hit each endpoint manually:

```bash
# 100,924 links and 1,239,946 speed records loaded
SELECT COUNT(*) FROM links;
SELECT COUNT(*) FROM speed_records;

# All segments for Monday AM Peak
GET /aggregates/?day=Monday&period=AM+Peak

# Single segment — San Marco Blvd, ~21 mph
GET /aggregates/1002482095?day=Monday&period=AM+Peak

# Spatial filter around downtown Jacksonville
POST /aggregates/spatial_filter/
{"day":"Monday","period":"AM Peak","bbox":[-81.70,30.30,-81.60,30.40]}

# Slow link pattern — N Hammock Oaks Dr averaging 0.6 mph during AM Peak
GET /patterns/slow_links/?period=AM+Peak&threshold=25&min_days=1
```

All four endpoints came back with clean, correct GeoJSON responses.

---

## The bigger point

AI coding tools are genuinely useful, but they're a force multiplier on the engineer using them, not a replacement for engineering judgment. The things that made this system good — the index strategy, the geometry serialization approach, the ORM/SQL split, the healthcheck — those were all decided before Claude Code wrote anything. The AI moved fast on the implementation. I made sure it was implementing the right thing.
