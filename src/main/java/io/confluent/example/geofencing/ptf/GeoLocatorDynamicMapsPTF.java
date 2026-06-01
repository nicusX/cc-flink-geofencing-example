package io.confluent.example.geofencing.ptf;
import io.confluent.example.geofencing.maps.NamedArea;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.annotation.ArgumentTrait.SET_SEMANTIC_TABLE;

public class GeoLocatorDynamicMapsPTF extends ProcessTableFunction<Row> {
    private static final Logger LOGGER = LogManager.getLogger(GeoLocatorDynamicMapsPTF.class);


    private transient GeometryFactory geometryFactory;

    /**
     * Object holding all the NamedAreas of a location, by name.
     * Note that as of June 2026 CC Flink does not support MapState yet, so we wrap the NamedArea into a map and store it as ValueState.
     */
    public static class NamedAreaMapState {
        public Map<String, NamedArea> namedAreas = new HashMap<>();
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
     *  If the parsing fails throws IllegalArgumentException
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

            Long lastDetectedTs = row.getFieldAs("lastDetectedTs");
            if (lastDetectedTs == null) {
                throw new IllegalArgumentException("lastDetectedTs is missing");
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

    /**
     * This method is called when the operator is initialized. It must be used to initialize any resource reused across
     * invocations.
     */
    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        geometryFactory = new GeometryFactory();
    }



    public void eval(
            @StateHint NamedAreaMapState locationNamedAreas,
            @ArgumentHint(SET_SEMANTIC_TABLE) Row namedAreaRow,
            @ArgumentHint(SET_SEMANTIC_TABLE) Row itemRow
    ) {

        // If NamedArea row, update the state and emit no record
        if( namedAreaRow != null ) {
            try {
                String locationId = parseStringFieldByName(namedAreaRow,"location_id");
                NamedArea namedArea = parseNamedAreaRow(namedAreaRow);

                // Add/update the area to the location maps
                if (namedArea != null) {
                    locationNamedAreas.namedAreas.put(locationId, namedArea);

                    // DO NOT log on every record in production
                    LOGGER.info("Updating location map. Location:{}, area:{}", locationId, namedArea.name);
                } else {
                    LOGGER.warn("Unable to update the location map: {}", locationId);
                }
            } catch (IllegalArgumentException iae) {
                LOGGER.warn("Exception while parsing a named area", iae);
                throw new RuntimeException("Exception while parsing a named area map", iae);
            }

        }

        // TODO If  Item row, geolocate it
        if( itemRow != null) {
            // TODO catch IllegalArgumentException and log

            String locationId = parseStringFieldByName(itemRow,"location_id");
            Item item = parseItemRow(itemRow);

            if( item != null) {
                // TODO geo-fence the item and emit a row
            } else {
                LOGGER.warn("Unable to geo-fence an item at location {}", locationId);
            }
        }
    }
}
