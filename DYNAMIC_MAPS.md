## Dynamic Maps Geofencing example

This example shows how to implement geofencing with dynamic maps using a PTF.

The PTF expects two input streams:
1. Item: a single item to be located, in a specific location (`placeId` + `floorNumber`)
2. NamedArea maps: a map of a Named Area in a specific `placeId` & `floorNumber`

> Note: both tables are append-only and have no PRIMARY KEY, because at the moment PTF in CC only support append-only changelog tables.

### CC Flink PTF Reference Documentation

* [Process Table Functions in Confluent Cloud for Apache Flink](https://docs.confluent.io/cloud/current/flink/concepts/process-table-functions.html)
* [Create a Process Table Function in Confluent Cloud for Apache Flink](https://docs.confluent.io/cloud/current/flink/how-to-guides/create-ptf.html)
* [Differences between OSS Flink PTF and CC Flink PTF](https://docs.confluent.io/cloud/current/flink/concepts/process-table-functions.html#differences-from-apache-flink)
* [Current limitations of CC Flink PTF](https://docs.confluent.io/cloud/current/flink/concepts/process-table-functions.html#process-table-function-limitations)

### Item data structure

Represents a single item to be located.

| Column           | Type     | Description                                                 |
|------------------|----------|-------------------------------------------------------------|
| `item_id`        | `STRING` | Unique identifier of the item                               |
| `location_id`    | `STRING` | Location identifier, in the form `<placeId>-<floorNumber>`  |
| `x`              | `DOUBLE` | X coordinate within the location                            |
| `y`              | `DOUBLE` | Y coordinate within the location                            |
| `lastDetectedTs` | `BIGINT` | Timestamp of last detection (epoch milliseconds)            |

```sql
CREATE TABLE items (
  `item_id`        STRING,
  `location_id`    STRING,
  `x`              DOUBLE,
  `y`              DOUBLE,
  `lastDetectedTs` BIGINT
) WITH (
  'value.format' = 'json-registry'
);
```

Example JSON record:

```json
{
  "item_id": "30360392A01003C40200084A",
  "location_id": "ES_0279-1",
  "x": 250.0,
  "y": 800.0,
  "lastDetectedTs": 1745000010000
}
```



### NamedArea map data structure

Represents NamedArea in a given location.

Fields:

| Column        | Type                             | Description                                          |
|---------------|----------------------------------|------------------------------------------------------|
| `location_id` | `STRING`                         | Location identifier, in the form `<placeId>-<floorNumber>` |
| `area_name`   | `STRING`                         | Name of the area within the location                 |
| `polygon`     | `ARRAY<ROW<x DOUBLE, y DOUBLE>>` | Closed ring of vertices defining the area boundary   |

The polygon is an ordered array of `(x, y)` coordinate pairs forming a closed ring (first and last vertex must be identical), matching the JTS `Polygon` convention.

```sql
CREATE TABLE named_area_maps (
  `location_id`  STRING NOT NULL,
  `area_name`    STRING NOT NULL,
  `polygon`      ARRAY<ROW<x DOUBLE, y DOUBLE>> NOT NULL
) WITH (
  'value.format' = 'json-registry'
);
```

Example JSON record (as produced by `json-registry` format):

```json
{
  "location_id": "ES_0279-0",
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

### Testing the PTF

#### Register the PTF

TODO

#### Invoke the PTF in a statement

TODO SQL SELECT invoking the PTF passing the two tables 

#### Generate Named Area Maps

TODO INSERT INTO named_area_maps with the map of a location

#### Send items to locate

TODO INSERT INTO items various items falling into one area

TODO INSERT INTO items  an item falling in no area

TODO INSERT INTO items various items with a location which we have no area defined for


