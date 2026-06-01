package io.confluent.example.geofencing.maps;

import java.util.List;

/**
 * Map of a location
 */
public class Location {
    /**
     * Location ID
     */
    public final String locationId;

    /**
     * NamedAreas defined in the location, keyed by NamedArea.name
     */
    public final List<NamedArea> areas;

    public Location(String locationId, final List<NamedArea> areasList) {
        this.locationId = locationId;
        this.areas = areasList;
    }
}
