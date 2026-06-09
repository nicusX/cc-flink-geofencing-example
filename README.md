## Geofencing in Confluent Cloud Flink with dynamic maps

This example shows how to implement geofencing with dynamic maps using a Flink Process Table Function (PTF).

The example has been tested in Confluent Cloud Flink.

The PTF locates each item streamed in a fast event stream on a floorplan map defining areas of a location.
Each item refers to a location and has x,y coordinates on the floorplan of the location.
The PTF identifies which area(s) the item coordinates fall in.


The PTF consumes two input streams, both partitioned by location ID:
1. Named area maps: a map of areas of a location, each with a specific name
2. Item: a single item to be located, in a specific location


The maps are preserved in Flink state. New areas can be created and existing areas can be updated by
sending records to the named area maps input.

When an item is processed, the PTF emits one or more records, one for each area of the location the item coordinates fall in.
If the coordinates fall in no area defined for that location, or the location is unknown, the PTF emits a record with 
the original item but no matching area.

> The implementation uses [JTS](https://github.com/locationtech/jts) for geolocating coordinates on the location maps.

A simple utility extracts the area definitions from a DXF file representing the floorplan. This is an external tool independent
of the PTF and is provided only for demonstration purposes. The implementation is not particularly robust and "just works"
for the provided example. 
Properly extracting area map definitions from DXF is out of scope for this example.


> Note that, if the floorplan is correctly defined, there should be no overlapping areas in a location and no item should
> ever fall in more than one area. However, the DXF format cannot guarantee there is no overlap. For this reason, the
> PTF implementation supports the case of coordinates falling in more than one area.

### Prerequisites

Requires **Java 17** and **Maven** to build the PTF artifact.

---

### Data structures

#### Item data structure

Represents a single item to be located.

| Column           | Type     | Description                                                 |
|------------------|----------|-------------------------------------------------------------|
| `item_id`        | `STRING` | Unique identifier of the item                               |
| `location_id`    | `STRING` | Location identifier, in the form `<placeId>-<floorNumber>`  |
| `x`              | `DOUBLE` | X coordinate within the location                            |
| `y`              | `DOUBLE` | Y coordinate within the location                            |
| `last_detected_ts` | `BIGINT` | Timestamp of last detection (epoch milliseconds)            |


Example JSON record:

```json
{
  "item_id": "30360392A01003C40200084A",
  "location_id": "LOC-0001",
  "x": 323531.0,
  "y": 371648.0,
  "last_detected_ts": 1745000010000
}
```

#### NamedArea map data structure

Represents a named area in a given location.

Fields:

| Column        | Type                             | Description                                          |
|---------------|----------------------------------|------------------------------------------------------|
| `location_id` | `STRING`                         | Location identifier, in the form `<placeId>-<floorNumber>` |
| `area_name`   | `STRING`                         | Name of the area within the location                 |
| `polygon`     | `ARRAY<ROW<x DOUBLE, y DOUBLE>>` | Closed ring of vertices defining the area boundary   |

The polygon is an ordered array of `(x, y)` coordinate pairs forming a closed ring (first and last vertex must be identical), matching the JTS `Polygon` convention.

Example of named area JSON record (as produced by `json-registry` format):

```json
{
  "location_id": "LOC-0001",
  "area_name": "OFFICES",
  "polygon": [
    {"x": 327912.6, "y": 373146.2},
    {"x": 330998.2, "y": 373146.2},
    {"x": 330998.2, "y": 367666.1},
    {"x": 327912.6, "y": 367666.1},
    {"x": 327912.6, "y": 373146.2}
  ]
}
```

#### PTF output

The PTF passes through the item fields and adds 3 columns indicating matching areas:

| Column                 | Type          | Description                                                      |
|------------------------|---------------|------------------------------------------------------------------|
| `area`                 | `STRING`      | Name of the matching area, or `NULL` if no area matches          |
| `matching_area_idx`    | `INT NOT NULL` | 1-based index of this area among all matches (0 if no match)    |
| `total_matching_areas` | `INT NOT NULL` | Total number of areas the item coordinates fall in (0 if none)  |

No record is emitted when an area map record is received.

----

### Extracting area maps from DXF files

The `dxfextractor.sh` script parses a DXF floorplan file and generates records for the `named_area_maps` table. The location ID is derived from the DXF filename (without the `.dxf` extension).

> Note: this DXF extractor is not really in scope with the PTF example. But we use it to extract the areas from a DXF for a realistic example.

Before running, make sure you have built the JAR:
```bash
mvn package
```

#### SQL output

The project contains an example DXF file, [`LOC-0001.dxf`](./LOC-0001.dxf), defining a floorplan with five named areas:

![LOC-0001 floorplan](LOC-0001.png)

Extract the areas defined in the DXF floorplan as an `INSERT INTO` SQL statement. This is what we will use to test the PTF.

```bash
./dxfextractor.sh LOC-0001.dxf sql
```

This prints a Flink SQL `INSERT INTO named_area_maps` statement with all areas extracted from the DXF file. 
Copy the output and paste it directly into the Confluent Cloud SQL Workspace to populate the `named_area_maps` table (see below).

The Location ID is the name of the DXF file, without the `.dxf` extension.

> This DXF Extractor is far from being general and robust. 
> It extracts the areas defined in layers with a name containing the string "IDEN".

#### JSON output

Alternatively, the extractor can also generate areas in JSON format. We will not use this to test the PTF.

```bash
./dxfextractor.sh LOC-0001.dxf json
```

This prints one JSON record per line (JSONL). This format is not used in the demo but can be useful for loading data via other tools or pipelines.

---

### Testing the PTF

We now want to test the PTF with some realistic data.

#### 1. Create the input tables

```sql
CREATE TABLE named_area_maps
(
    `location_id` STRING NOT NULL,
    `area_name`   STRING NOT NULL,
    `polygon`     ARRAY<ROW<x DOUBLE, y DOUBLE>> NOT NULL
) DISTRIBUTED BY HASH(`location_id`, `area_name`) INTO 6 BUCKETS
  WITH (
    'value.format' = 'json-registry',
    'changelog.mode' =  'append',
    'scan.startup.mode' = 'earliest-offset',
    'kafka.cleanup-policy' = 'compact' 
);
```
Notes:
* `DISTRIBUTED BY HASH(location_id, area_name)`: determines the partitioning of the source topic. 
    The PTF does not require partitioning the source. This is required if `compact` cleanup policy is used.
* `...INTO 6 BUCKETS`: determines the number of partitions in the topic. There is no specific requirement for the number of partitions.
* `'value.format' = 'json-registry'`: the PTF actually works with any format.
* `'changelog.mode' =  'append'`: mandatory. PTF only supports append changelog at the moment. 
    Note that `append` is the default when no primary key is defined, but we make it explicit for clarity.
* `'scan.startup.mode' = 'earliest-offset'`: force replaying the entire compacted topic when the statement is re-created with no state.
    This allows rebuilding all the maps in state.
* `'kafka.cleanup-policy' = 'compact'`: this is just an option. The PTF does not require compaction. 
    However, all the most recent maps for each `location_id` and `area_name` must be retained, to rebuild the state in case the statement 
    has to be recreated. If `delete` policy is used instead, you need infinite retention.

```sql
CREATE TABLE items (
    `item_id`        STRING,
    `location_id`    STRING,
    `x`              DOUBLE,
    `y`              DOUBLE,
    `last_detected_ts` BIGINT
) WITH (
    'value.format' = 'json-registry',
    'scan.startup.mode' = 'latest-offset',
    'changelog.mode' =  'append'
);
```

Notes:
* `'value.format' = 'json-registry'`: the PTF actually works with any format.
* `'changelog.mode' =  'append'`: mandatory. PTF only supports append changelog at the moment.
* `'scan.startup.mode' = 'latest-offset'`: this is not a requirement, and it's just useful for the demo


#### 2. Build and upload the PTF artifact (JAR)

Compile the PTF and build the JAR artifact. This also runs unit tests:

```bash
mvn package
```

The build generates the artifact in `./target/geofencing-example-1.0.jar` (do not use the jar named `original-*`).

Upload it in the Artifacts page in Confluent Cloud Environments. 
Make sure you upload it in the same cloud provider 
and region where you will be running the SQL statements.

#### 3. Register the PTF

Register the function:

```sql
CREATE FUNCTION GeoLocatorDynamicMaps
AS 'io.confluent.example.geofencing.ptf.GeoLocatorDynamicMapsPTF'
USING JAR 'confluent-artifact://<artifact-id>';
```

Replace `<artifact-id>` with the ID returned by the artifact upload.

#### 4. Invoke the PTF

Let's create a `SELECT` statement that invokes the PTF:

```sql
-- Query invoking the PTF on each incoming Item
SELECT 
    location_id, item_id, x, y, last_detected_ts, area, matching_area_idx, total_matching_areas
FROM GeoLocatorDynamicMaps(
  namedAreaRow => TABLE named_area_maps PARTITION BY location_id,
  itemRow      => TABLE items PARTITION BY location_id
);
```

Keep this query running. We will observe the output while we are sending maps and items.

The query emits the name of the matching area, and the index and total number of matching areas (remember: it can be more than one!).
It also passes through the item fields.

| Column                 | Description                                      |
|------------------------|--------------------------------------------------|
| `location_id`          | Location identifier (partition key, automatic)   |
| `item_id`              | Unique identifier of the item                    |
| `x`                    | X coordinate within the location                 |
| `y`                    | Y coordinate within the location                 |
| `last_detected_ts`       | Timestamp of last detection (epoch milliseconds) |
| `area`                 | Name of the matching area (NULL if none)          |
| `matching_area_idx`    | 1-based index among matching areas (0 if none)   |
| `total_matching_areas` | Total number of areas the point matched          |

No record is emitted when an area map is received.

#### 5. Load named area maps


Generate the `INSERT INTO` statement from a DXF file (see [Extracting area maps from DXF files](#extracting-area-maps-from-dxf-files)):

```bash
./dxfextractor.sh LOC-0001.dxf sql
```

This generates five area maps for the location `LOC-0001`: *MAIN CLASSROOM*, *CLASSROOM 1*, *CLASSROOM 2*, *CLASSROOM 3* and *OFFICES*.

Copy the output `INSERT INTO ...` statement and paste it into the SQL Workspace.

No output record is emitted by the PTF when it processes new maps.

#### 6. Send items to locate


**Items falling in exactly one area** (inside *MAIN CLASSROOM* and *CLASSROOM 3* respectively):

```sql
-- Two items, falling in two different areas
INSERT INTO items (item_id, location_id, x, y, last_detected_ts)
VALUES
  ('ITEM-001', 'LOC-0001', 323531.0, 371648.0, 1745000010000),
  ('ITEM-002', 'LOC-0001', 333196.0, 378000.0, 1745000011000);
```

Expected output: one row per item, `area = 'MAIN CLASSROOM'` and `area = 'CLASSROOM 3'` respectively, both with `matching_area_idx = 1` and `total_matching_areas = 1`.

**Item falling in no area** (outside all zones):

```sql
-- Item falling in no defined area
INSERT INTO items (item_id, location_id, x, y, last_detected_ts)
VALUES
  ('ITEM-003', 'LOC-0001', 350000.0, 370000.0, 1745000012000);
```

Expected output: one row with `area = NULL`, `matching_area_idx = 0`, `total_matching_areas = 0`.

**Item at an unknown location** (no area maps loaded for this location):

```sql
-- Item in unknown location
INSERT INTO items (item_id, location_id, x, y, last_detected_ts)
VALUES
  ('ITEM-004', 'LOC-UNKNOWN', 50000.0, 50000.0, 1745000013000);
```

Expected output: one row with `area = NULL`, `matching_area_idx = 0`, `total_matching_areas = 0` (no areas in state for this partition).

#### 7. Add another location

Let's add the maps of another location, on the fly. No need to stop the PTF.

We use a synthetic location, not from a DXF file, for this test.
The new location `LOC-0002` also has a map error: there are two overlapping areas.

```
         0               80000   120000          200000
  200000 ┌───────────────────────┐
         │         AREA1         │
  120000 │               ┌───────┼───────────────┐
         │               │OVERLAP│               │
  100000 │               │       │               │
   80000 └───────────────┼───────┘               │
                         │         AREA2         │
       0                 └───────────────────────┘

         AREA3: x[300000,400000], y[0,100000]  (separate)
```


```sql
-- Map of a new location
INSERT INTO named_area_maps (location_id, area_name, polygon)
VALUES
  ('LOC-0002', 'AREA1', ARRAY[ROW(0.0, 80000.0), ROW(120000.0, 80000.0), ROW(120000.0, 200000.0), ROW(0.0, 200000.0), ROW(0.0, 80000.0)]),
  ('LOC-0002', 'AREA2', ARRAY[ROW(80000.0, 0.0), ROW(200000.0, 0.0), ROW(200000.0, 120000.0), ROW(80000.0, 120000.0), ROW(80000.0, 0.0)]),
  ('LOC-0002', 'AREA3', ARRAY[ROW(300000.0, 0.0), ROW(400000.0, 0.0), ROW(400000.0, 100000.0), ROW(300000.0, 100000.0), ROW(300000.0, 0.0)]);
```

**Item in *AREA3* only (no overlap):**

```sql
-- Item falling in an area of location LOC-0002
INSERT INTO items (item_id, location_id, x, y, last_detected_ts)
VALUES
  ('ITEM-011', 'LOC-0002', 350000.0, 50000.0, 1745000021000);
```
Expected output: one row with `area = 'AREA3'`, `matching_area_idx = 1`, `total_matching_areas = 1`.

**Item in the overlap between *AREA1* and *AREA2*:**

```sql
-- Item falling in two areas of location LOC-0002
INSERT INTO items (item_id, location_id, x, y, last_detected_ts)
VALUES
  ('ITEM-010', 'LOC-0002', 100000.0, 100000.0, 1745000020000);
```

Expected output: two rows for ITEM-010, one with `area = 'AREA1'` and one with `area = 'AREA2'`, both with `total_matching_areas = 2`.

---

### Features of the PTF implementation

All maps are retained in Flink state. If the statement is stopped (or crashes) and then resumed, the maps are restored from state.

If you need to re-create the statement, this will clear the state. To rebuild it, replay the entire content of `named_area_maps`.
For this reason, this topic should have infinite retention or it should use compaction, publishing records with the composite
key `(location_id, area_name)` to retain the latest record for each area.

The PTF supports updating existing areas: when a new `named_area_maps` record for a `location_id` and `area_name` is received, it replaces the old
area in Flink state. Deleting existing named area maps is not supported, but handling tombstone records (e.g. records with an empty `polygon`)
should be trivial.

The PTF logs to INFO every time a map is added or updated.
It also logs to INFO on every item located for demonstration purposes, but logging on each record in production is bad practice. 

The PTF implements no particular error handling. If any exception is thrown, the problem is logged but the exception is rethrown.
This causes the Flink statement to stop.

---


### CC Flink PTF Reference Documentation

* [Process Table Functions in Confluent Cloud for Apache Flink](https://docs.confluent.io/cloud/current/flink/concepts/process-table-functions.html)
* [Create a Process Table Function in Confluent Cloud for Apache Flink](https://docs.confluent.io/cloud/current/flink/how-to-guides/create-ptf.html)
* [Differences between OSS Flink PTF and CC Flink PTF](https://docs.confluent.io/cloud/current/flink/concepts/process-table-functions.html#differences-from-apache-flink)
* [Current limitations of CC Flink PTF](https://docs.confluent.io/cloud/current/flink/concepts/process-table-functions.html#process-table-function-limitations)
