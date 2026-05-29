package io.confluent.example.udtf.geofencing.maps;

import java.util.List;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import static org.assertj.core.api.Assertions.assertThat;

class GeoLocatorTest {

    private final GeoLocator geoLocator = new GeoLocator();
    private final GeometryFactory gf = new GeometryFactory();

    private Polygon rectangle(double x1, double y1, double x2, double y2) {
        return gf.createPolygon(new Coordinate[]{
                new Coordinate(x1, y1),
                new Coordinate(x2, y1),
                new Coordinate(x2, y2),
                new Coordinate(x1, y2),
                new Coordinate(x1, y1)
        });
    }

    @Nested
    class PointInsideOneArea {

        private final Location map = new Location("LOC1", List.of(
                new NamedArea("A", rectangle(0, 0, 10, 10)),
                new NamedArea("B", rectangle(20, 20, 30, 30))
        ));

        @Test
        void shouldReturnAreaA() {
            List<String> result = geoLocator.locateAreas(map, 5, 5);
            assertThat(result).containsExactly("A");
        }

        @Test
        void shouldReturnAreaB() {
            List<String> result = geoLocator.locateAreas(map, 25, 25);
            assertThat(result).containsExactly("B");
        }
    }

    @Nested
    class PointOutsideAllAreas {

        private final Location map = new Location("LOC1", List.of(
                new NamedArea("A", rectangle(0, 0, 10, 10))
        ));

        @Test
        void shouldReturnEmptyList() {
            List<String> result = geoLocator.locateAreas(map, 50, 50);
            assertThat(result).isEmpty();
        }
    }

    @Nested
    class PointInsideOverlappingAreas {

        private final Location map = new Location("LOC1", List.of(
                new NamedArea("A", rectangle(0, 0, 20, 20)),
                new NamedArea("B", rectangle(5, 5, 25, 25))
        ));

        @Test
        void shouldReturnBothAreas() {
            List<String> result = geoLocator.locateAreas(map, 10, 10);
            assertThat(result).containsExactlyInAnyOrder("A", "B");
        }

        @Test
        void shouldReturnOnlyAWhenInNonOverlappingPartOfA() {
            List<String> result = geoLocator.locateAreas(map, 2, 2);
            assertThat(result).containsExactly("A");
        }
    }

    @Nested
    class EmptyMap {

        private final Location map = new Location("EMPTY", List.of());

        @Test
        void shouldReturnEmptyList() {
            List<String> result = geoLocator.locateAreas(map, 5, 5);
            assertThat(result).isEmpty();
        }
    }
}
