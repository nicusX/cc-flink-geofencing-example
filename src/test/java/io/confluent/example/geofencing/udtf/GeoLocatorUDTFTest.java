package io.confluent.example.geofencing.udtf;

import io.confluent.example.geofencing.maps.GeoLocator;
import io.confluent.example.geofencing.maps.NamedArea;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

class GeoLocatorUDTFTest {

    private final GeometryFactory gf = new GeometryFactory();
    private GeoLocatorUDTF udtf;
    private List<Row> collected;

    private Polygon rectangle(double x1, double y1, double x2, double y2) {
        return gf.createPolygon(new Coordinate[]{
                new Coordinate(x1, y1),
                new Coordinate(x2, y1),
                new Coordinate(x2, y2),
                new Coordinate(x1, y2),
                new Coordinate(x1, y1)
        });
    }

    @BeforeEach
    void setUp() throws Exception {
        udtf = new GeoLocatorUDTF();
        collected = new ArrayList<>();

        Map<String, List<NamedArea>> locations = Map.of(
                "LOC1", List.of(
                        new NamedArea("A", rectangle(0, 0, 10, 10)),
                        new NamedArea("B", rectangle(20, 20, 30, 30))
                ),
                "LOC_OVERLAP", List.of(
                        new NamedArea("X", rectangle(0, 0, 20, 20)),
                        new NamedArea("Y", rectangle(5, 5, 25, 25))
                )
        );

        setPrivateField(GeoLocatorUDTF.class, udtf, "locations", locations);
        setPrivateField(GeoLocatorUDTF.class, udtf, "locator", new GeoLocator(new GeometryFactory()));
        installTestCollector(udtf);
    }

    private void setPrivateField(Class<?> clazz, Object target, String fieldName, Object value) throws Exception {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private void installTestCollector(GeoLocatorUDTF target) throws Exception {
        Field collectorField = TableFunction.class.getDeclaredField("collector");
        collectorField.setAccessible(true);
        collectorField.set(target, new Collector<Row>() {
            @Override
            public void collect(Row row) {
                collected.add(row);
            }

            @Override
            public void close() {}
        });
    }

    @Nested
    class PointInsideOneArea {

        @Test
        void shouldEmitSingleRowWithCorrectFields() {
            udtf.eval("LOC1", 5.0, 5.0);

            assertThat(collected).singleElement().satisfies(row -> {
                assertThat(row.getField(0)).isEqualTo("A");
                assertThat(row.getField(1)).isEqualTo(1);
                assertThat(row.getField(2)).isEqualTo(1);
            });
        }

        @Test
        void shouldEmitAreaB() {
            udtf.eval("LOC1", 25.0, 25.0);

            assertThat(collected).singleElement().satisfies(row ->
                    assertThat(row.getField(0)).isEqualTo("B"));
        }
    }

    @Nested
    class PointOutsideAllAreas {

        @Test
        void shouldEmitNoRows() {
            udtf.eval("LOC1", 50.0, 50.0);
            assertThat(collected).isEmpty();
        }
    }

    @Nested
    class UnknownLocation {

        @Test
        void shouldEmitNoRows() {
            udtf.eval("NONEXISTENT", 5.0, 5.0);
            assertThat(collected).isEmpty();
        }
    }

    @Nested
    class OverlappingAreas {

        @Test
        void shouldEmitTwoRowsWithCorrectIndicesAndTotal() {
            udtf.eval("LOC_OVERLAP", 10.0, 10.0);

            assertThat(collected)
                    .extracting(r -> r.getField(0), r -> r.getField(1), r -> r.getField(2))
                    .containsExactly(
                            tuple("X", 1, 2),
                            tuple("Y", 2, 2)
                    );
        }

        @Test
        void shouldEmitOneRowWhenOnlyInNonOverlappingPart() {
            udtf.eval("LOC_OVERLAP", 2.0, 2.0);

            assertThat(collected).singleElement().satisfies(row -> {
                assertThat(row.getField(0)).isEqualTo("X");
                assertThat(row.getField(1)).isEqualTo(1);
                assertThat(row.getField(2)).isEqualTo(1);
            });
        }
    }
}
