package io.confluent.example.geofencing.udtf;

import io.confluent.example.geofencing.maps.GeoLocator;
import io.confluent.example.geofencing.maps.LocationLoader;
import io.confluent.example.geofencing.maps.NamedArea;
import org.locationtech.jts.geom.GeometryFactory;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GeoLocatorUDTFIntegrationTest {

    private GeoLocatorUDTF udtf;
    private List<Row> collected;

    @BeforeAll
    void loadMaps() throws Exception {
        udtf = new GeoLocatorUDTF();

        LocationLoader loader = new LocationLoader();
        Map<String, List<NamedArea>> locations = loader.loadAllMapsFromResources();

        setPrivateField(GeoLocatorUDTF.class, udtf, "locations", locations);
        setPrivateField(GeoLocatorUDTF.class, udtf, "locator", new GeoLocator(new GeometryFactory()));
    }

    @BeforeEach
    void resetCollector() throws Exception {
        collected = new ArrayList<>();
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
    class Stockroom1 {

        @Test
        void centerOfStockroom1() {
            udtf.eval("ES_0279", 2050.0, 25300.0);

            assertThat(collected).singleElement().satisfies(row -> {
                assertThat((String) row.getField(0)).startsWith("STOCKROOM 1");
                assertThat(row.getField(1)).isEqualTo(1);
                assertThat(row.getField(2)).isEqualTo(1);
            });
        }

        @Test
        void topOfStore() {
            udtf.eval("ES_0279", 3000.0, 30000.0);

            assertThat(collected).singleElement().satisfies(row ->
                    assertThat((String) row.getField(0)).startsWith("STOCKROOM 1"));
        }

        @Test
        void leftSideUpperFloor() {
            udtf.eval("ES_0279", 1000.0, 25000.0);

            assertThat(collected).singleElement().satisfies(row ->
                    assertThat((String) row.getField(0)).startsWith("STOCKROOM 1"));
        }
    }

    @Nested
    class Frontstore {

        @Test
        void centerOfFrontstore() {
            udtf.eval("ES_0279", 5300.0, 22000.0);

            assertThat(collected).singleElement().satisfies(row -> {
                assertThat((String) row.getField(0)).startsWith("FRONTSTORE");
                assertThat(row.getField(1)).isEqualTo(1);
                assertThat(row.getField(2)).isEqualTo(1);
            });
        }

        @Test
        void rightSideUpperFloor() {
            udtf.eval("ES_0279", 7000.0, 25000.0);

            assertThat(collected).singleElement().satisfies(row ->
                    assertThat((String) row.getField(0)).startsWith("FRONTSTORE"));
        }
    }

    @Nested
    class Stockroom2 {

        @Test
        void centerOfStockroom2() {
            udtf.eval("ES_0279", 4050.0, 11250.0);

            assertThat(collected).singleElement().satisfies(row ->
                    assertThat((String) row.getField(0)).startsWith("STOCKROOM 2"));
        }
    }

    @Nested
    class Stockroom3 {

        @Test
        void centerOfStockroom3() {
            udtf.eval("ES_0279", 3840.0, 3070.0);

            assertThat(collected).singleElement().satisfies(row ->
                    assertThat((String) row.getField(0)).startsWith("STOCKROOM 3"));
        }
    }

    @Nested
    class OutsideAllAreas {

        @Test
        void gapBetweenFrontstoreAndStockroom2() {
            udtf.eval("ES_0279", 7000.0, 15000.0);
            assertThat(collected).isEmpty();
        }

        @Test
        void gapBetweenStockroom1AndFrontstore() {
            udtf.eval("ES_0279", 2500.0, 20000.0);
            assertThat(collected).isEmpty();
        }

        @Test
        void farOutsideBuilding() {
            udtf.eval("ES_0279", 50000.0, 50000.0);
            assertThat(collected).isEmpty();
        }

        @Test
        void negativeCoordinates() {
            udtf.eval("ES_0279", -5000.0, -5000.0);
            assertThat(collected).isEmpty();
        }
    }

    @Nested
    class UnknownLocation {

        @Test
        void shouldEmitNoRows() {
            udtf.eval("NONEXISTENT", 100.0, 100.0);
            assertThat(collected).isEmpty();
        }
    }
}
