package io.confluent.example.geofencing.ptf;

import io.confluent.example.geofencing.ptf.GeoLocatorDynamicMapsPTF.PolygonPOJO;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import static org.assertj.core.api.Assertions.assertThat;

class GeoLocatorDynamicMapsPTF_PolygonPOJOTest {

    private final GeometryFactory gf = new GeometryFactory();

    private Polygon rectangle(double x1, double y1, double x2, double y2) {
        return gf.createPolygon(new Coordinate[]{
                new Coordinate(x1, y1), new Coordinate(x2, y1),
                new Coordinate(x2, y2), new Coordinate(x1, y2),
                new Coordinate(x1, y1)
        });
    }

    @Test
    void noArgConstructorShouldCreateEmptyArrays() {
        PolygonPOJO pojo = new PolygonPOJO();

        assertThat(pojo.xs).isEmpty();
        assertThat(pojo.ys).isEmpty();
    }

    @Test
    void fromPolygonShouldPreserveCoordinates() {
        Polygon polygon = rectangle(10.0, 20.0, 30.0, 40.0);

        PolygonPOJO pojo = PolygonPOJO.fromPolygon(polygon);

        assertThat(pojo.xs).containsExactly(10.0, 30.0, 30.0, 10.0, 10.0);
        assertThat(pojo.ys).containsExactly(20.0, 20.0, 40.0, 40.0, 20.0);
    }

    @Test
    void toPolygonShouldRecreateOriginalPolygon() {
        Polygon original = rectangle(100.0, 200.0, 300.0, 400.0);

        PolygonPOJO pojo = PolygonPOJO.fromPolygon(original);
        Polygon restored = pojo.toPolygon(gf);

        assertThat(restored.equalsExact(original)).isTrue();
    }

    @Test
    void roundTripShouldPreserveArea() {
        Polygon original = rectangle(0.0, 0.0, 50.0, 50.0);

        Polygon restored = PolygonPOJO.fromPolygon(original).toPolygon(gf);

        assertThat(restored.getArea()).isEqualTo(original.getArea());
    }

    @Test
    void roundTripShouldPreserveValidity() {
        Polygon original = rectangle(0.0, 0.0, 10.0, 10.0);

        Polygon restored = PolygonPOJO.fromPolygon(original).toPolygon(gf);

        assertThat(restored.isValid()).isTrue();
    }

    @Test
    void shouldHandleComplexPolygon() {
        Polygon triangle = gf.createPolygon(new Coordinate[]{
                new Coordinate(0.0, 0.0), new Coordinate(10.0, 0.0),
                new Coordinate(5.0, 8.66), new Coordinate(0.0, 0.0)
        });

        PolygonPOJO pojo = PolygonPOJO.fromPolygon(triangle);
        Polygon restored = pojo.toPolygon(gf);

        assertThat(pojo.xs).hasSize(4);
        assertThat(restored.equalsExact(triangle)).isTrue();
    }

    @Test
    void shouldPreserveContainmentAfterRoundTrip() {
        Polygon original = rectangle(0.0, 0.0, 100.0, 100.0);
        Polygon restored = PolygonPOJO.fromPolygon(original).toPolygon(gf);

        assertThat(restored.contains(gf.createPoint(new Coordinate(50.0, 50.0)))).isTrue();
        assertThat(restored.contains(gf.createPoint(new Coordinate(200.0, 200.0)))).isFalse();
    }
}
