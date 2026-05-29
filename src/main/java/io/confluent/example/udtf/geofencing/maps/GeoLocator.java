package io.confluent.example.udtf.geofencing.maps;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.util.ArrayList;
import java.util.List;

/**
 * Locate a point on a Location
 */
public class GeoLocator {
    // Reuse a single GeometryFactory to avoid unnecessary object creation
    private final GeometryFactory geometryFactory = new GeometryFactory();

    /**
     * Locate a point with coordinate x,y on the map of a location.
     * Determine which area(s) the point is.
     *
     * @param map Location of the location
     * @param x coordinate
     * @param y coordinate
     * @return a List containing zero or more names of areas where the point is. The list is empty if the point does not fall in any named area.
     */
    public List<String> locateAreas(Location map, double x, double y) {
        List<String> matchedAreaNames = new ArrayList<>();

        // Create a JTS Point from the coordinates
        Point queryPoint = geometryFactory.createPoint(new Coordinate(x, y));

        for(NamedArea area : map.areas) {
            // Check if the polygon contains the point
            if (area.polygon.contains(queryPoint)) {
                matchedAreaNames.add(area.name);
            }
        }

        return matchedAreaNames;
    }
}
