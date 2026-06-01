package io.confluent.example.geofencing.maps;

import org.locationtech.jts.geom.Polygon;

/**
 * Defines an area on a map, with a name
 */
public class NamedArea {
    /**
     * Name of the area
     */
    public final String name;

    /**
     * Polygon defining the area
     */
    public final Polygon polygon;

    public NamedArea(String name, Polygon polygon) {
        this.name = name;
        this.polygon = polygon;
    }
}
