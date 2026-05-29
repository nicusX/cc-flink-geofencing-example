# Geofencing UDTF for Confluent Cloud Flink

A Flink **User Defined Table Function** (UDTF) that determines which area(s) of a store/warehouse a given point falls into. Floorplan boundaries are defined as DXF files bundled inside the JAR and loaded at startup.

## How it works

1. At startup (`open()`), the function loads all DXF floorplan maps from `src/main/resources/maps/` and caches them in memory. Each file is named after its location ID (e.g. `ES_0279.dxf`).
2. For each input record, the function receives a `location_id` and `(x, y)` coordinates, and performs a point-in-polygon test against all named areas defined in that location's DXF map.
3. It emits **one row per matching area**. If the point falls outside all areas, no row is emitted.

### Input

| Argument      | Type     | Description                         |
|---------------|----------|-------------------------------------|
| `location_id` | `STRING` | Identifier of the location/store    |
| `x`           | `DOUBLE` | X coordinate within the floorplan   |
| `y`           | `DOUBLE` | Y coordinate within the floorplan   |

### Output

| Column                 | Type              | Description                                             |
|------------------------|-------------------|---------------------------------------------------------|
| `area`                 | `STRING`          | Name of the matching area                               |
| `matching_area_idx`    | `INT NOT NULL`    | 1-based index of this area among all matches            |
| `total_matching_areas` | `INT NOT NULL`    | Total number of areas the point matched                 |

In normal conditions a point matches exactly one area (`matching_area_idx = 1`, `total_matching_areas = 1`). Overlapping polygons in DXF maps can produce multiple matches.

## Build

Requires **Java 17** and **Maven**.

```bash
mvn clean package       # compile, test, and produce the uber-jar
mvn test                # run tests only
```

The build produces an uber-jar in `target/` via the maven-shade-plugin. All runtime dependencies (Kabeja DXF parser, JTS geometry) are bundled. Flink-provided dependencies (`flink-table-api-java`, `log4j-api`) are excluded to avoid version clashes with the Confluent Cloud runtime.

## Adding a new location

Place the DXF floorplan file in `src/main/resources/maps/` with the location ID as the filename (e.g. `US_1042.dxf`), then rebuild the JAR. The function discovers all `.dxf` files in that folder automatically at startup.

DXF requirements:
- Area boundaries must be `LWPOLYLINE` entities on layers containing `A-AREA-BNDY` in the name.
- Area labels must be `MTEXT` or `TEXT` entities on layers containing `IDEN` in the name. The text insertion point must fall inside the corresponding boundary polygon.

## Testing on Confluent Cloud Flink

The following SQL instructions let you register the function, set up a test table, insert sample data, and query the UDTF interactively.

### 1. Upload and register the function

Upload the uber-jar from `target/` to Confluent Cloud, then register the function in your Flink SQL workspace:

```sql
CREATE FUNCTION GeoLocator
  AS 'GeoLocatorUDTF'
  USING JAR '<artifact-id>';
```

Verify the function is available:

```sql
DESCRIBE FUNCTION GeoLocator;
```

### 2. Create a test input table

```sql
CREATE TABLE epc_locations (
  `epc`           STRING,
  `location_id`   STRING,
  `x`             DOUBLE,
  `y`             DOUBLE
) WITH (
  'value.format' = 'json-registry'
);
```

### 3. Insert sample data

These coordinates correspond to known areas in the bundled `ES_0279.dxf` map:

```sql
INSERT INTO epc_locations VALUES
  ('30360392A01003C40200084A', 'ES_0279', 2050,  25300),  -- Stockroom 1
  ('3036039B30107C4404000232', 'ES_0279', 5300,  22000),  -- Frontstore
  ('3036039B30107BC404000D7E', 'ES_0279', 4050,  11250),  -- Stockroom 2
  ('30360390E84F09C4040017D0', 'ES_0279', 3840,  3070),   -- Stockroom 3
  ('30360390E8434DC40200053E', 'ES_0279', 50000, 50000);  -- Outside all areas
```

### 4. Query with the UDTF

Use `LEFT JOIN LATERAL` so that records with no matching area are preserved (with `NULL` area fields) rather than silently dropped:

```sql
SELECT
  e.epc,
  e.location_id,
  e.x,
  e.y,
  g.area,
  g.matching_area_idx,
  g.total_matching_areas
FROM epc_locations AS e
LEFT JOIN LATERAL TABLE(GeoLocator(e.location_id, e.x, e.y)) AS g ON TRUE;
```

Expected output:

| epc                        | location_id | x     | y     | area         | matching_area_idx | total_matching_areas |
|----------------------------|-------------|-------|-------|--------------|-------------------|----------------------|
| 30360392A01003C40200084A   | ES_0279     | 2050  | 25300 | STOCKROOM 1… | 1                 | 1                    |
| 3036039B30107C4404000232   | ES_0279     | 5300  | 22000 | FRONTSTORE…  | 1                 | 1                    |
| 3036039B30107BC404000D7E   | ES_0279     | 4050  | 11250 | STOCKROOM 2… | 1                 | 1                    |
| 30360390E84F09C4040017D0   | ES_0279     | 3840  | 3070  | STOCKROOM 3… | 1                 | 1                    |
| 30360390E8434DC40200053E   | ES_0279     | 50000 | 50000 | NULL         | NULL              | NULL                 |

### 5. Filter to only matched areas

If you only want records that matched an area, use `CROSS JOIN LATERAL` (or `JOIN LATERAL ... ON TRUE`) instead:

```sql
SELECT
  e.epc,
  g.area
FROM epc_locations AS e
CROSS JOIN LATERAL TABLE(GeoLocator(e.location_id, e.x, e.y)) AS g;
```

### 6. Clean up

```sql
DROP TABLE epc_locations;
DROP FUNCTION GeoLocator;
```

---

## Next steps

### Dynamic maps

Make the map dynamic instead of packaging them in the JAR and preloading on start.

This require using a PTF with set semantics (`PARTITION BY location_id`) and expecting two tables as input: 
1. EPC to be located
2. Maps of locations' areas (compacted, partitioned by `location_id`)

Note that, because the EPC record contains the `location_id`, we do not need support for broadcast state, and we can co-partition maps and epc partitioning both by `location_id`.

The schema of the Maps topic is TBD. 
It must represent a Polygon associated with a `location_id` and `area_name`.
org.locationtech.jts.geom.Polygon must be serialized/deserialized as JSON.

This also requires an external tool to parse the DXF files and publish the maps of the areas into the Maps topic.

### Including the floor dimension

The current implementation does not consider the `floor` dimension.
This  is just an additional key to be added to EPC records and also to Maps.


### Unfold multi-EPC batch messages

The [message example](./example_batch_multifloor.json) contains multiple EPC records in the same payload.
The UDTF (and the PTF) expects in input a record containing  a single EPC in a single location (and floor).

The batch message should be unfolded in SQL before calling the function.