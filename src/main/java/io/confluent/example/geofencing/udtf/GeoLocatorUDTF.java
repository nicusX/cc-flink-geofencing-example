package io.confluent.example.geofencing.udtf;

import io.confluent.example.geofencing.maps.GeoLocator;
import io.confluent.example.geofencing.maps.LocationLoader;
import io.confluent.example.geofencing.maps.NamedArea;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Flink Table User Defined Function to determine which area(s) of a specific location a point is.
 *
 * The function expects as input the id of the area and (x,y) coordinates within the map of the area.
 * In nominal conditions a point  should always fall in a single area.
 * However, maps defined as DXF do not guarantee that areas never overlap. For this reason, the implementation is designed
 * to return zero or more matching areas.
 *
 * If any matching area is found, the UDTF returns a record for each area
 * - Name of the area
 * - Sequential index of the matching area, starting from 1 (not zero!)
 * - Total number of matching areas (the same number in all the returned records)
 *
 * If no matching area is found, the UDTF does not return any record, and logs to WARN (because this should not happen)
 * Not to miss the non-matching record the UDTF should be  used as LEFT JOIN LATERAL
 *
 */
@FunctionHint(
        output = @DataTypeHint("ROW<`area` STRING, `matching_area_idx` INT NOT NULL, `total_matching_areas` INT NOT NULL>"))
public class GeoLocatorUDTF extends TableFunction<Row> {
    private static final Logger LOGGER = LogManager.getLogger(GeoLocatorUDTF.class);

    // Maps of all locations
    private transient Map<String, List<NamedArea>> locations;

    // Locator of points within a Location
    private transient GeoLocator locator;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        // Load the location maps
        LOGGER.info("Loading location maps");
        LocationLoader loader = new LocationLoader();
        this.locations = loader.loadAllMapsFromResources();
        LOGGER.info("Loaded {} locations", locations.size());

        // Initialize the locator
        this.locator = new GeoLocator();
    }

    public void eval(
            @ArgumentHint(name = "location_id", type = @DataTypeHint("STRING")) String locationId,
            @ArgumentHint(name = "x", type = @DataTypeHint("DOUBLE")) Double x,
            @ArgumentHint(name = "y", type = @DataTypeHint("DOUBLE")) Double y
    ) {
        // ATTENTION: you should not log on every record in production
        LOGGER.info("Geo-locating ({},{}) in location {}", x, y, locationId);

        // Get the map of the location
        if( !locations.containsKey(locationId)) {
            LOGGER.warn("No map available for location {}", locationId);
            // Return no record
            return;
        }

        // Get the map of the location
        List<NamedArea> areas = locations.get(locationId);

        // Locate the point
        List<String> matchingAreas = locator.locateAreas(areas, x, y);

        int matchingAreaCount = matchingAreas.size();
        if( matchingAreaCount > 0 ) {
            // Emit one row for each matching area
            for (int i = 0; i < matchingAreaCount; i++) {
                collect(Row.of(
                        matchingAreas.get(i), // Name of the matching area
                        i + 1, // Index starts from 1
                        matchingAreaCount // Total number of matching areas
                ));
            }
        } else {
            // No matching area: it shouldn't happen, so we log to WARN
            LOGGER.warn("No matching area for ({},{}) in location {}", x, y, locationId);
            // Return no record
        }

    }

}
