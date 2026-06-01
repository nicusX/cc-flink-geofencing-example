package io.confluent.example.geofencing.ptf;

import io.confluent.example.geofencing.maps.GeoLocator;
import io.confluent.example.geofencing.maps.NamedArea;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.annotation.ArgumentTrait.SET_SEMANTIC_TABLE;

/**
 * PTF implementing geofencing via dynamic maps.
 * <p>
 * Expects two input tables, both {@code PARTITIONED BY location_id}:
 * <p>
 * 1. Named area maps ({@code named_area_maps}):
 * <pre>
 *   location_id  STRING   -- location identifier ({@code <placeId>-<floorNumber>})
 *   area_name    STRING   -- name of the area
 *   polygon      ARRAY&lt;ROW&lt;x DOUBLE, y DOUBLE&gt;&gt;  -- closed ring of vertices
 * </pre>
 * <p>
 * 2. Detected items ({@code detected_items}):
 * <pre>
 *   item_id        STRING  -- unique item identifier
 *   location_id    STRING  -- location identifier ({@code <placeId>-<floorNumber>})
 *   x              DOUBLE  -- X coordinate within the location
 *   y              DOUBLE  -- Y coordinate within the location
 *   last_detected_ts BIGINT  -- timestamp of last detection (epoch milliseconds)
 * </pre>
 * <p>
 * Output row (item fields are emitted explicitly since {@code PASS_COLUMNS_THROUGH}
 * is not supported with multiple input tables; {@code location_id} is omitted because
 * the partition key is automatically passed through by the PTF framework):
 * <pre>
 *   item_id              STRING    -- item identifier (from item input)
 *   x                    DOUBLE    -- X coordinate (from item input)
 *   y                    DOUBLE    -- Y coordinate (from item input)
 *   last_detected_ts     BIGINT    -- last detection timestamp (from item input)
 *   area                 STRING    -- name of the matching area
 *   matching_area_idx    INT       -- 1-based index of this area among all matches
 *   total_matching_areas INT       -- total number of areas the point matched
 * </pre>
 * <p>
 * Note that the PTF automatically pass-through the partition column of each input table.
 * The output will contain two columns with location_id.
 * <p>
 * Named area maps are preserved in state. Every time a named area map record is received,
 * the state representing the map of the location (all named areas at the location) is updated.
 * The state is a map keyed by area name. When an area map record is received, it replaces any
 * pre-existing area with the same name at the same location. Otherwise, the area is added.
 * No record is returned in this case.
 * <p>
 * When an item record is received, the PTF geo-positions it on the map of the location.
 * One row is emitted for each named area where the item coordinates fall.
 * Under normal conditions the item should fall in exactly one area. However, because the DXF
 * maps do not guarantee that areas never overlap nor that the entire floorplan is covered,
 * zero or multiple matches are possible.
 * <p>
 * If the item does not fall in any named area, return a single record with
 * null matching area name.
 * Conversely, if the item falls into multiple areas, multiple rows are emitted.
 *
 * <p>
 * If any error happens while parsing the input records, the function should log a message to
 * WARN and return no record.
 *
 * Note that the PTF "manually" pass-through the Item record. PTF has a trait called PASS_COLUMNS_THROUGH which
 * automatically pass-through the input record. However, PASS_COLUMNS_THROUGH is not supported for multi-table PTF.
 */
@DataTypeHint("ROW<`item_id` STRING, `x` DOUBLE, `y` DOUBLE, `last_detected_ts` BIGINT, `area` STRING, `matching_area_idx` INT NOT NULL, `total_matching_areas` INT NOT NULL>")
public class GeoLocatorDynamicMapsPTF extends ProcessTableFunction<Row> {
    private static final Logger LOGGER = LogManager.getLogger(GeoLocatorDynamicMapsPTF.class);


    // These properties are initialized in open() and reused across invocations
    private transient GeometryFactory geometryFactory;
    private transient GeoLocator geoLocator;


    /**
     * Class holding all the areas of a location, by name.
     * Note that as of June 2026 CC Flink does not support MapState yet, so we wrap the POJO into a Map and store it as ValueState.
     */
    public static class LocationMapState {
        public Map<String, PolygonPOJO> locationMapState = new HashMap<>();
    }

    /**
     * Flink-serializable representation of a JTS Polygon (all public fields, no-args constructor).
     * Stores the same content as a JTS Polygon, i.e. coordinates as parallel arrays of x and y values.
     * However, JTS Polygon is not a POJO and we cannot use it directly in Flink state.
     */
    public static class PolygonPOJO {

        public double[] xs;
        public double[] ys;

        public PolygonPOJO() {
            this.xs = new double[0];
            this.ys = new double[0];
        }

        private PolygonPOJO(double[] xs, double[] ys) {
            this.xs = xs;
            this.ys = ys;
        }

        public static PolygonPOJO fromPolygon(Polygon polygon) {
            Coordinate[] coords = polygon.getCoordinates();
            double[] xs = new double[coords.length];
            double[] ys = new double[coords.length];
            for (int i = 0; i < coords.length; i++) {
                xs[i] = coords[i].x;
                ys[i] = coords[i].y;
            }
            return new PolygonPOJO(xs, ys);
        }

        public Polygon toPolygon(GeometryFactory geometryFactory) {
            Coordinate[] coords = new Coordinate[xs.length];
            for (int i = 0; i < xs.length; i++) {
                coords[i] = new Coordinate(xs[i], ys[i]);
            }
            return geometryFactory.createPolygon(coords);
        }
    }

    /**
     * Represents a record in the item table
     */
    @VisibleForTesting
    static class Item {
        public String itemId;
        public Double x;
        public Double y;
        public Instant lastDetectedTs;
    }

    /**
     * Parse the Row containing the named area into a NamedArea object.
     * If the parsing fails throws IllegalArgumentException
     */
    @VisibleForTesting
    NamedArea parseNamedAreaRow(Row row) {
        if (row == null) {
            throw new IllegalArgumentException("Named area row is null");
        }

        // Parse the record into a NamedArea
        try {
            String areaName = parseStringFieldByName(row, "area_name");

            Row[] polygonRows = row.getFieldAs("polygon");
            if (polygonRows == null) {
                throw new IllegalArgumentException("polygon is missing");
            }

            if (polygonRows.length < 4) {
                throw new IllegalArgumentException(
                        "polygon must have at least 4 vertices (closed ring), got " + polygonRows.length);
            }

            Coordinate[] coordinates = new Coordinate[polygonRows.length];
            for (int i = 0; i < polygonRows.length; i++) {
                Row point = polygonRows[i];
                if (point == null) {
                    throw new IllegalArgumentException("polygon vertex at index " + i + " is null");
                }

                Double x = point.getFieldAs("x");
                Double y = point.getFieldAs("y");
                if (x == null) {
                    throw new IllegalArgumentException("polygon vertex " + i + ": x is missing");
                }
                if (y == null) {
                    throw new IllegalArgumentException("polygon vertex " + i + ": y is missing");
                }

                coordinates[i] = new Coordinate(x, y);
            }

            if (!coordinates[0].equals2D(coordinates[coordinates.length - 1])) {
                throw new IllegalArgumentException("polygon is not closed (first and last vertex must be identical)");
            }

            Polygon polygon = geometryFactory.createPolygon(coordinates);
            return new NamedArea(areaName, polygon);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Field has unexpected type: " + e.getMessage(), e);
        }
    }


    /**
     * Parse a row containing the item to locate into an Item object
     * If the parsing fails throws IllegalArgumentException
     */
    @VisibleForTesting
    Item parseItemRow(Row row) {
        if (row == null) {
            throw new IllegalArgumentException("Item row is null");
        }

        try {
            Item item = new Item();

            item.itemId = parseStringFieldByName(row, "item_id");

            item.x = row.getFieldAs("x");
            if (item.x == null) {
                throw new IllegalArgumentException("x is missing");
            }

            item.y = row.getFieldAs("y");
            if (item.y == null) {
                throw new IllegalArgumentException("y is missing");
            }

            Long lastDetectedTs = row.getFieldAs("last_detected_ts");
            if (lastDetectedTs == null) {
                throw new IllegalArgumentException("last_detected_ts is missing");
            }
            item.lastDetectedTs = Instant.ofEpochMilli(lastDetectedTs);

            return item;
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Field has unexpected type: " + e.getMessage(), e);
        }
    }

    /**
     * Extract a String field from a Row by name. Throw IllegalArgumentException if the field is not present or
     * cannot be cast to a String
     */
    @VisibleForTesting
    String parseStringFieldByName(Row row, String fieldName) {
        try {
            String value = row.getFieldAs(fieldName);
            if (value == null) {
                throw new IllegalArgumentException("Field " + fieldName + " is missing");
            }
            return value;
        } catch (ClassCastException cce) {
            throw new IllegalArgumentException("Field " + fieldName + " is not a string", cce);
        }
    }

    @VisibleForTesting
    GeometryFactory getGeometryFactory() {
        return geometryFactory;
    }


    //////////////////////////////////////////
    //// Implementation of the PTF interface
    //////////////////////////////////////////

    /**
     * This method is called when the operator is initialized. It must be used to initialize any resource reused across
     * invocations.
     */
    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        geometryFactory = new GeometryFactory();
        geoLocator = new GeoLocator(geometryFactory);
    }


    /**
     * Main PTF method: invoked when a record from either tables is processed.
     * Because both tables have set semantics, they must be both PARTITIONED BY the same key: location_id.
     * The state visible in the method is also implicitly partitioned by the same key.
     */
    public void eval(
            @StateHint LocationMapState locationNamedAreas,
            @ArgumentHint(SET_SEMANTIC_TABLE) Row namedAreaRow,
            @ArgumentHint(SET_SEMANTIC_TABLE) Row itemRow
    ) {

        /// Handle namedArea row, update the state and emit no record
        if (namedAreaRow != null) {

            // Parse the area
            String locationId = null;
            NamedArea namedArea;
            try {
                locationId = parseStringFieldByName(namedAreaRow, "location_id");
                namedArea = parseNamedAreaRow(namedAreaRow);

                // If it was unable to parse the area, for any reason, throw an exception
                if (namedArea == null) {
                    throw new IllegalArgumentException("Unable to parse the named area map at location " + locationId);
                }
            } catch (IllegalArgumentException iae) {
                // On validation exception: Log and rethrow to make the specific exception visible in the logs
                LOGGER.warn("Exception while parsing a named area at location " + locationId, iae);
                throw iae;
            }

            // Add/update the area to the location maps (keyed by area name)
            locationNamedAreas.locationMapState.put(namedArea.name, PolygonPOJO.fromPolygon(namedArea.polygon));

            // DO NOT log on every record in production
            LOGGER.info("Updating location map. Location:{}, area:{}", locationId, namedArea.name);
            // Return no record
        }

        //.Handle Item row, geolocate it
        if (itemRow != null) {

            // Parse the item to locate
            String locationId = null;
            Item item = null;
            try {
                locationId = parseStringFieldByName(itemRow, "location_id");
                item = parseItemRow(itemRow);

                // If it was unable to parse an item, for any reason, throw an exception
                if (item == null) {
                    throw new IllegalArgumentException("Unable to parse the item at location " + locationId);
                }
            } catch (IllegalArgumentException iae) {
                // On validation exception: Log and rethrow to make the specific exception visible in the logs
                LOGGER.warn("Exception while parsing an item to locate at location " + locationId, iae);
                throw iae;
            }

            // Reconstruct NamedArea objects from state POJOs for geolocation
            List<NamedArea> areas = locationNamedAreas.locationMapState.entrySet().stream()
                    .map(e -> new NamedArea(e.getKey(), e.getValue().toPolygon(geometryFactory)))
                    .toList();

            // Geolocate the item on the location map: find the named areas the item coordinates fall in
            List<String> matchingAreas = this.geoLocator.locateAreas(areas, item.x, item.y);

            // DO NOT log on every record in production
            LOGGER.info("Item {} located in {} areas within location {}", item.itemId, matchingAreas.size(), locationId);

            // Return one row for each matching area
            int matchingAreaCount = matchingAreas.size();
            Long lastDetectedEpoch = item.lastDetectedTs.toEpochMilli();

            if (matchingAreaCount > 0) {
                // Emit one row for each matching area
                for (int i = 0; i < matchingAreaCount; i++) {
                    collect(Row.of(
                            item.itemId, item.x, item.y, lastDetectedEpoch,
                            matchingAreas.get(i), i + 1, matchingAreaCount
                    ));
                }
            } else {
                // If the item coordinate do not fall in any area, return the item (pass-through) and null matching area_name
                collect(Row.of(
                        item.itemId, item.x, item.y, lastDetectedEpoch,
                        null, 0, 0
                ));
            }
        }
    }
}
