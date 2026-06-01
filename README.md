## Geofencing in Confluent Cloud Flink with dynamic maps

This example shows how to implement geofencing with dynamic maps using a PTF.

The PTF locates each item streamed in a fast event stream on a floorplan map defining areas of a location.
Each item refers ot a location and  has x,y coordinates on the floorplan of the location 
The PTF identifies which area(s) the item coordinates fall in. 


The PTF expects two input streams:
1. Named area maps: a map of a areas of a location, each with a specific name
2. Item: a single item to be located, in a specific location

> Note: both tables are append-only and have no PRIMARY KEY, because at the moment PTF in CC only support append-only changelog tables.

The maps of locations are preserved in Flink state. New areas can be created and existing areas can be updated 
sending records to the named area maps input.

When an Item is processed, the PTF emits one or more records, for each area of the location the item coordinates fall in.
If the coordinates fall in no area defined for that location, or the location is unknown, the PTF emits a record with 
the original Item but no matching area.

> The implementation uses [JTS](https://github.com/locationtech/jts) for geolocating coordinates on the location maps.

> [Kabeja](https://kabeja.sourceforge.net/) is used to parse the DXF floorplan files and extract the polygons that defined the
> areas. This is a very old and not very robust library. However, a robust DXF parsing is out of scope for this example.
> The PTF implementation relies on maps defined as polygons, never mind the way they are extracted. Kabeja is only used to 
> parse the provided DXF to provide a realistic example.

> Note that, if the floorplan is correctly defined, there should be no overlapping areas in a location and no item should
> ever fall in more than one area. However, the DXF format cannot guarantee there is no overlap. For this reason, the
> PTF implementation support the case of coordinates falling in more than one area.

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
| `lastDetectedTs` | `BIGINT` | Timestamp of last detection (epoch milliseconds)            |


Example JSON record:

```json
{
  "item_id": "30360392A01003C40200084A",
  "location_id": "ES_0279-1F",
  "x": 250.0,
  "y": 800.0,
  "lastDetectedTs": 1745000010000
}
```

#### NamedArea map data structure

Represents NamedArea in a given location.

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
  "location_id": "ES_0279-1F",
  "area_name": "STOCKROOM 1",
  "polygon": [
    {"x": 0.0, "y": 20000.0},
    {"x": 4000.0, "y": 20000.0},
    {"x": 4000.0, "y": 27000.0},
    {"x": 0.0, "y": 27000.0},
    {"x": 0.0, "y": 20000.0}
  ]
}
```

#### PTF output

The PTF pass through the Item and adds 3 columns indicating matching areas:

| Column                 | Type          | Description                                                      |
|------------------------|---------------|------------------------------------------------------------------|
| `area`                 | `STRING`      | Name of the matching area, or `NULL` if no area matches          |
| `matching_area_idx`    | `INT NOT NULL` | 1-based index of this area among all matches (0 if no match)    |
| `total_matching_areas` | `INT NOT NULL` | Total number of areas the item coordinates fall in (0 if none)  |

Note: no record is returned when an area map is received.

----

### Extracting area maps from DXF files

The `dxfextractor.sh` script parses a DXF floorplan file and generates records for the `named_area_maps` table. The location ID is derived from the DXF filename (without the `.dxf` extension).

> Note: this DXF extractor is not really in scope with the PTF example. But we use it to extract the areas from a DXF for a realistic example.

Before running, make sure you have built the JAR:
```bash
mvn package
```

#### SQL output

Extract the areas defined in the DXF floorplan in form of an INSERT INTO SQL statement. This is what we will use to test the PTF.

```bash
./dxfextractor.sh ES_0279-1F.dxf sql
```

This prints a Flink SQL `INSERT INTO named_area_maps` statement with all areas extracted from the DXF file. 
Copy the output and paste it directly into the Confluent Cloud SQL Workspace to populate the `named_area_maps` table (see below).

The Location ID is the name of the DXF file, without the `.dxf` extension.

#### JSON output

Alternatively, the extractor can also generate areas in JSON format. We will not use this to test the PTF.

```bash
./dxfextractor.sh ES_0279-1F.dxf json
```

This prints one JSON record per line (JSONL). This format is not used in the demo but can be useful for loading data via other tools or pipelines.

---

### Testing the PTF

We now want to test the PTF with some realistic data.

#### 1. Create the input tables

```sql
CREATE TABLE named_area_maps (
  `location_id`  STRING NOT NULL,
  `area_name`    STRING NOT NULL,
  `polygon`      ARRAY<ROW<x DOUBLE, y DOUBLE>> NOT NULL
) WITH (
  'value.format' = 'json-registry',
  'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE items (
  `item_id`        STRING,
  `location_id`    STRING,
  `x`              DOUBLE,
  `y`              DOUBLE,
  `lastDetectedTs` BIGINT
) WITH (
  'value.format' = 'json-registry',
  'scan.startup.mode' = 'latest-offset'
);
```

#### 2. Build and upload the PTF artifact (JAR)

```bash
mvn package
```

Upload it in the Artifact page in the Confluent Cloud Environments. Make sure you upload it in the same cloud provider 
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

```sql
SELECT *
FROM GeoLocatorDynamicMaps(
  namedAreaRow => TABLE named_area_maps PARTITION BY location_id,
  itemRow      => TABLE items PARTITION BY location_id
);
```

Keep this query running. We will observe the output while we are sending maps and items.

The query emits all the name of the matching area, and the index and total number of matching areas (remember: it can be more than 1!).
It also pass-through the Item.

| Column                 | Description                                      |
|------------------------|--------------------------------------------------|
| `item_id`              | Unique identifier of the item                    |
| `location_id`          | Location identifier                              |
| `x`                    | X coordinate within the location                 |
| `y`                    | Y coordinate within the location                 |
| `lastDetectedTs`       | Timestamp of last detection (epoch milliseconds) |
| `area`                 | Name of the matching area (NULL if none)          |
| `matching_area_idx`    | 1-based index among matching areas (0 if none)   |
| `total_matching_areas` | Total number of areas the point matched          |

No record is emitted when a area map is received.

#### 5. Load named area maps


Generate the `INSERT INTO` statement from a DXF file (see [Extracting area maps from DXF files](#extracting-area-maps-from-dxf-files)):

```bash
./dxfextractor.sh ES_0279-1F.dxf sql
```

This generates three areas maps for the location `ES_0279-1F`.

Copy the output and paste it into the SQL Workspace.
You should observe no output from the PTF.

#### 6. Send items to locate


**Items falling in exactly one area** (inside *STOCKROOM 2* and *FRONTSTORE* respectively):

```sql
INSERT INTO items (item_id, location_id, x, y, lastDetectedTs)
VALUES
  ('ITEM-001', 'ES_0279-1F', 4051.3, 11247.7, 1745000010000),
  ('ITEM-002', 'ES_0279-1F', 5000.0, 28000.0, 1745000011000);
```

Expected output: one row per item, `area = 'STOCKROOM 2'` and `area = 'FRONTSTORE'` respectively, both with `matching_area_idx = 1` and `total_matching_areas = 1`.

**Item falling in no area** (outside all zones):

```sql
INSERT INTO items (item_id, location_id, x, y, lastDetectedTs)
VALUES
  ('ITEM-003', 'ES_0279-1F', 15000.0, 15000.0, 1745000012000);
```

Expected output: one row with `area = NULL`, `matching_area_idx = 0`, `total_matching_areas = 0`.

**Item at an unknown location** (no area maps loaded for this location):

```sql
INSERT INTO items (item_id, location_id, x, y, lastDetectedTs)
VALUES
  ('ITEM-004', 'UNKNOWN-1F', 50.0, 50.0, 1745000013000);
```

Expected output: one row with `area = NULL`, `matching_area_idx = 0`, `total_matching_areas = 0` (no areas in state for this partition).

#### 7. Test another location with overlapping areas

Load three rectangular areas for location `LOC42-2F`. *AREA1* and *AREA2* overlap in the region (80,80)–(120,120):

```
         0       80  100 120     200
    200  ┌────────────────┐
         │    AREA1       │
    120  │        ┌───────┼──────┐
         │        │OVERLAP│      │
    100  │        │       │      │
     80  └────────┼───────┘      │
         ·        │    AREA2     │
      0  ·        └──────────────┘
         0       80             200
                        AREA3: (300,0)–(400,100)  (separate)
```


```sql
INSERT INTO named_area_maps (location_id, area_name, polygon)
VALUES
  ('LOC42-2F', 'AREA1', ARRAY[ROW(0.0, 80.0), ROW(120.0, 80.0), ROW(120.0, 200.0), ROW(0.0, 200.0), ROW(0.0, 80.0)]),
  ('LOC42-2F', 'AREA2', ARRAY[ROW(80.0, 0.0), ROW(200.0, 0.0), ROW(200.0, 120.0), ROW(80.0, 120.0), ROW(80.0, 0.0)]),
  ('LOC42-2F', 'AREA3', ARRAY[ROW(300.0, 0.0), ROW(400.0, 0.0), ROW(400.0, 100.0), ROW(300.0, 100.0), ROW(300.0, 0.0)]);
```

**Item in the overlap between *AREA1* and *AREA2*:**

```sql
INSERT INTO items (item_id, location_id, x, y, lastDetectedTs)
VALUES
  ('ITEM-010', 'LOC42-2F', 100.0, 100.0, 1745000020000);
```

Expected output: two rows for ITEM-010, one with `area = 'AREA1'` and one with `area = 'AREA2'`, both with `total_matching_areas = 2`.

**Item in *AREA3* only (no overlap):**

```sql
INSERT INTO items (item_id, location_id, x, y, lastDetectedTs)
VALUES
  ('ITEM-011', 'LOC42-2F', 350.0, 50.0, 1745000021000);
```

Expected output: one row with `area = 'AREA3'`, `matching_area_idx = 1`, `total_matching_areas = 1`.

---


### CC Flink PTF Reference Documentation

* [Process Table Functions in Confluent Cloud for Apache Flink](https://docs.confluent.io/cloud/current/flink/concepts/process-table-functions.html)
* [Create a Process Table Function in Confluent Cloud for Apache Flink](https://docs.confluent.io/cloud/current/flink/how-to-guides/create-ptf.html)
* [Differences between OSS Flink PTF and CC Flink PTF](https://docs.confluent.io/cloud/current/flink/concepts/process-table-functions.html#differences-from-apache-flink)
* [Current limitations of CC Flink PTF](https://docs.confluent.io/cloud/current/flink/concepts/process-table-functions.html#process-table-function-limitations)
