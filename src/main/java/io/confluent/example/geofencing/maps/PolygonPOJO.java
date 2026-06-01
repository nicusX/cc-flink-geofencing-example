package io.confluent.example.geofencing.maps;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

/**
 * Flink-serializable POJO representation of a JTS Polygon.
 * Stores coordinates as parallel arrays of x and y values.
 */
public class PolygonPOJO {

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
