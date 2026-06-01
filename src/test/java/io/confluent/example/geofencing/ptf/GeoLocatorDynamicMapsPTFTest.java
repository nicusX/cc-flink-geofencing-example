package io.confluent.example.geofencing.ptf;

import io.confluent.example.geofencing.maps.NamedArea;
import io.confluent.example.geofencing.maps.PolygonPOJO;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

class GeoLocatorDynamicMapsPTFTest {

    private final GeoLocatorDynamicMapsPTF ptf = new GeoLocatorDynamicMapsPTF();
    private List<Row> collected;

    @BeforeEach
    void setUp() throws Exception {
        ptf.open(null);
        collected = new ArrayList<>();
        ptf.setCollector(new Collector<>() {
            @Override
            public void collect(Row row) { collected.add(row); }
            @Override
            public void close() {}
        });
    }

    @Nested
    class ParseNamedAreaRow {

        @Test
        void shouldParseValidNamedAreaRow() {
            Row row = Row.withNames();
            row.setField("area_name", "STOCKROOM 1");
            row.setField("polygon", new Row[]{
                    point(0.0, 0.0),
                    point(10.0, 0.0),
                    point(10.0, 10.0),
                    point(0.0, 10.0),
                    point(0.0, 0.0)
            });

            NamedArea result = ptf.parseNamedAreaRow(row);

            assertThat(result.name).isEqualTo("STOCKROOM 1");
            assertThat(result.polygon.isValid()).isTrue();
            assertThat(result.polygon.getCoordinates()).hasSize(5);
        }

        @Test
        void shouldPreservePolygonCoordinates() {
            Row row = Row.withNames();
            row.setField("area_name", "ZONE A");
            row.setField("polygon", new Row[]{
                    point(100.0, 200.0),
                    point(300.0, 200.0),
                    point(300.0, 400.0),
                    point(100.0, 400.0),
                    point(100.0, 200.0)
            });

            NamedArea result = ptf.parseNamedAreaRow(row);

            Coordinate[] coords = result.polygon.getCoordinates();
            assertThat(coords[0]).isEqualTo(new Coordinate(100.0, 200.0));
            assertThat(coords[1]).isEqualTo(new Coordinate(300.0, 200.0));
            assertThat(coords[2]).isEqualTo(new Coordinate(300.0, 400.0));
            assertThat(coords[3]).isEqualTo(new Coordinate(100.0, 400.0));
            assertThat(coords[4]).isEqualTo(new Coordinate(100.0, 200.0));
        }

        @Test
        void shouldThrowOnNullRow() {
            assertThatThrownBy(() -> ptf.parseNamedAreaRow(null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("null");
        }

        @Test
        void shouldThrowOnMissingAreaName() {
            Row row = Row.withNames();
            row.setField("polygon", new Row[]{
                    point(0.0, 0.0), point(10.0, 0.0),
                    point(10.0, 10.0), point(0.0, 0.0)
            });

            assertThatThrownBy(() -> ptf.parseNamedAreaRow(row))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("area_name");
        }

        @Test
        void shouldThrowOnNonStringAreaName() {
            Row row = Row.withNames();
            row.setField("area_name", 42);
            row.setField("polygon", new Row[]{
                    point(0.0, 0.0), point(10.0, 0.0),
                    point(10.0, 10.0), point(0.0, 10.0),
                    point(0.0, 0.0)
            });

            assertThatThrownBy(() -> ptf.parseNamedAreaRow(row))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("area_name")
                    .hasCauseInstanceOf(ClassCastException.class);
        }

        @Test
        void shouldThrowOnMissingPolygon() {
            Row row = Row.withNames();
            row.setField("area_name", "ZONE A");

            assertThatThrownBy(() -> ptf.parseNamedAreaRow(row))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("polygon");
        }

        @Test
        void shouldThrowOnTooFewVertices() {
            Row row = Row.withNames();
            row.setField("area_name", "ZONE A");
            row.setField("polygon", new Row[]{
                    point(0.0, 0.0), point(10.0, 0.0), point(0.0, 0.0)
            });

            assertThatThrownBy(() -> ptf.parseNamedAreaRow(row))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("at least 4");
        }

        @Test
        void shouldThrowOnUnclosedPolygon() {
            Row row = Row.withNames();
            row.setField("area_name", "ZONE A");
            row.setField("polygon", new Row[]{
                    point(0.0, 0.0), point(10.0, 0.0),
                    point(10.0, 10.0), point(0.0, 10.0)
            });

            assertThatThrownBy(() -> ptf.parseNamedAreaRow(row))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("not closed");
        }

        @Test
        void shouldThrowOnNullVertex() {
            Row row = Row.withNames();
            row.setField("area_name", "ZONE A");
            row.setField("polygon", new Row[]{
                    point(0.0, 0.0), null,
                    point(10.0, 10.0), point(0.0, 10.0),
                    point(0.0, 0.0)
            });

            assertThatThrownBy(() -> ptf.parseNamedAreaRow(row))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("null");
        }

        @Test
        void shouldThrowOnMissingXCoordinate() {
            Row badPoint = Row.withNames();
            badPoint.setField("y", 10.0);

            Row row = Row.withNames();
            row.setField("area_name", "ZONE A");
            row.setField("polygon", new Row[]{
                    point(0.0, 0.0), badPoint,
                    point(10.0, 10.0), point(0.0, 10.0),
                    point(0.0, 0.0)
            });

            assertThatThrownBy(() -> ptf.parseNamedAreaRow(row))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("x is missing");
        }

        @Test
        void shouldThrowOnMissingYCoordinate() {
            Row badPoint = Row.withNames();
            badPoint.setField("x", 10.0);

            Row row = Row.withNames();
            row.setField("area_name", "ZONE A");
            row.setField("polygon", new Row[]{
                    point(0.0, 0.0), badPoint,
                    point(10.0, 10.0), point(0.0, 10.0),
                    point(0.0, 0.0)
            });

            assertThatThrownBy(() -> ptf.parseNamedAreaRow(row))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("y is missing");
        }

        private Row point(double x, double y) {
            Row point = Row.withNames();
            point.setField("x", x);
            point.setField("y", y);
            return point;
        }
    }

    @Nested
    class ParseItemRow {

        @Test
        void shouldParseValidItemRow() {
            Row row = Row.withNames();
            row.setField("item_id", "30360392A01003C40200084A");
            row.setField("x", 250.0);
            row.setField("y", 800.0);
            row.setField("lastDetectedTs", 1745000010000L);

            GeoLocatorDynamicMapsPTF.Item result = ptf.parseItemRow(row);

            assertThat(result.itemId).isEqualTo("30360392A01003C40200084A");
            assertThat(result.x).isEqualTo(250.0);
            assertThat(result.y).isEqualTo(800.0);
            assertThat(result.lastDetectedTs).isEqualTo(Instant.ofEpochMilli(1745000010000L));
        }

        @Test
        void shouldThrowOnNullRow() {
            assertThatThrownBy(() -> ptf.parseItemRow(null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("null");
        }

        @Test
        void shouldThrowOnMissingItemId() {
            Row row = Row.withNames();
            row.setField("x", 250.0);
            row.setField("y", 800.0);
            row.setField("lastDetectedTs", 1745000010000L);

            assertThatThrownBy(() -> ptf.parseItemRow(row))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("item_id");
        }

        @Test
        void shouldThrowOnMissingX() {
            Row row = Row.withNames();
            row.setField("item_id", "EPC1");
            row.setField("y", 800.0);
            row.setField("lastDetectedTs", 1745000010000L);

            assertThatThrownBy(() -> ptf.parseItemRow(row))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("x is missing");
        }

        @Test
        void shouldThrowOnMissingY() {
            Row row = Row.withNames();
            row.setField("item_id", "EPC1");
            row.setField("x", 250.0);
            row.setField("lastDetectedTs", 1745000010000L);

            assertThatThrownBy(() -> ptf.parseItemRow(row))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("y is missing");
        }

        @Test
        void shouldThrowOnMissingLastDetectedTs() {
            Row row = Row.withNames();
            row.setField("item_id", "EPC1");
            row.setField("x", 250.0);
            row.setField("y", 800.0);

            assertThatThrownBy(() -> ptf.parseItemRow(row))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("lastDetectedTs");
        }

        @Test
        void shouldThrowOnWrongType() {
            Row row = Row.withNames();
            row.setField("item_id", 123);
            row.setField("x", 250.0);
            row.setField("y", 800.0);
            row.setField("lastDetectedTs", 1745000010000L);

            assertThatThrownBy(() -> ptf.parseItemRow(row))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("item_id")
                    .hasCauseInstanceOf(ClassCastException.class);
        }
    }

    @Nested
    class ParseStringFieldByName {

        @Test
        void shouldReturnStringValue() {
            Row row = Row.withNames();
            row.setField("name", "hello");

            assertThat(ptf.parseStringFieldByName(row, "name")).isEqualTo("hello");
        }

        @Test
        void shouldThrowOnMissingField() {
            Row row = Row.withNames();
            row.setField("other", "value");

            assertThatThrownBy(() -> ptf.parseStringFieldByName(row, "name"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("name")
                    .hasMessageContaining("missing");
        }

        @Test
        void shouldThrowOnNullValue() {
            Row row = Row.withNames();
            row.setField("name", null);

            assertThatThrownBy(() -> ptf.parseStringFieldByName(row, "name"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("name")
                    .hasMessageContaining("missing");
        }

        @Test
        void shouldThrowOnWrongType() {
            Row row = Row.withNames();
            row.setField("name", 42);

            assertThatThrownBy(() -> ptf.parseStringFieldByName(row, "name"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("name")
                    .hasCauseInstanceOf(ClassCastException.class);
        }
    }

    @Nested
    class Eval {

        private GeoLocatorDynamicMapsPTF.LocationMapState state;

        @BeforeEach
        void initState() {
            state = new GeoLocatorDynamicMapsPTF.LocationMapState();
        }

        // --- Named area map handling ---

        @Test
        void namedAreaRowShouldUpdateStateAndEmitNoRecord() {
            ptf.eval(state, namedAreaRow("LOC1", "ZONE A", 0, 0, 10, 10), null);

            assertThat(collected).isEmpty();
            assertThat(state.locationMapState).containsKey("ZONE A");
        }

        @Test
        void multipleAreasShouldAllBePreservedInState() {
            ptf.eval(state, namedAreaRow("LOC1", "ZONE A", 0, 0, 10, 10), null);
            ptf.eval(state, namedAreaRow("LOC1", "ZONE B", 20, 20, 30, 30), null);

            assertThat(state.locationMapState).hasSize(2);
            assertThat(state.locationMapState).containsKeys("ZONE A", "ZONE B");
        }

        @Test
        void namedAreaWithSameNameShouldReplaceExisting() {
            ptf.eval(state, namedAreaRow("LOC1", "ZONE A", 0, 0, 10, 10), null);
            ptf.eval(state, namedAreaRow("LOC1", "ZONE A", 0, 0, 50, 50), null);

            assertThat(state.locationMapState).hasSize(1);
            PolygonPOJO polygonPojo = state.locationMapState.get("ZONE A");
            assertThat(polygonPojo.toPolygon(ptf.getGeometryFactory()).getArea()).isEqualTo(2500.0);
        }

        // --- Item geolocation ---

        @Test
        void itemInsideOneAreaShouldEmitOneRow() {
            ptf.eval(state, namedAreaRow("LOC1", "ZONE A", 0, 0, 10, 10), null);
            ptf.eval(state, namedAreaRow("LOC1", "ZONE B", 20, 20, 30, 30), null);

            ptf.eval(state, null, itemRow("LOC1", "EPC1", 5.0, 5.0));

            assertThat(collected).singleElement().satisfies(row -> {
                assertThat(row.getField(0)).isEqualTo("EPC1");
                assertThat(row.getField(1)).isEqualTo("LOC1");
                assertThat(row.getField(2)).isEqualTo(5.0);
                assertThat(row.getField(3)).isEqualTo(5.0);
                assertThat(row.getField(4)).isEqualTo(1745000010000L);
                assertThat(row.getField(5)).isEqualTo("ZONE A");
                assertThat(row.getField(6)).isEqualTo(1);
                assertThat(row.getField(7)).isEqualTo(1);
            });
        }

        @Test
        void itemInsideOverlappingAreasShouldEmitMultipleRows() {
            ptf.eval(state, namedAreaRow("LOC1", "X", 0, 0, 20, 20), null);
            ptf.eval(state, namedAreaRow("LOC1", "Y", 5, 5, 25, 25), null);

            ptf.eval(state, null, itemRow("LOC1", "EPC1", 10.0, 10.0));

            assertThat(collected)
                    .extracting(r -> r.getField(0), r -> r.getField(5), r -> r.getField(6), r -> r.getField(7))
                    .containsExactly(
                            tuple("EPC1", "X", 1, 2),
                            tuple("EPC1", "Y", 2, 2)
                    );
        }

        @Test
        void itemOutsideAllAreasShouldEmitRowWithNullArea() {
            ptf.eval(state, namedAreaRow("LOC1", "ZONE A", 0, 0, 10, 10), null);

            ptf.eval(state, null, itemRow("LOC1", "EPC1", 50.0, 50.0));

            assertThat(collected).singleElement().satisfies(row -> {
                assertThat(row.getField(0)).isEqualTo("EPC1");
                assertThat(row.getField(1)).isEqualTo("LOC1");
                assertThat(row.getField(5)).isNull();
                assertThat(row.getField(6)).isEqualTo(0);
                assertThat(row.getField(7)).isEqualTo(0);
            });
        }

        @Test
        void itemWithNoAreasLoadedShouldEmitRowWithNullArea() {
            ptf.eval(state, null, itemRow("LOC1", "EPC1", 5.0, 5.0));

            assertThat(collected).singleElement().satisfies(row -> {
                assertThat(row.getField(0)).isEqualTo("EPC1");
                assertThat(row.getField(1)).isEqualTo("LOC1");
                assertThat(row.getField(5)).isNull();
                assertThat(row.getField(6)).isEqualTo(0);
                assertThat(row.getField(7)).isEqualTo(0);
            });
        }

        // --- Error handling ---

        @Test
        void invalidNamedAreaRowShouldThrow() {
            Row badRow = Row.withNames();
            badRow.setField("location_id", "LOC1");

            assertThatThrownBy(() -> ptf.eval(state, badRow, null))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(collected).isEmpty();
        }

        @Test
        void invalidItemRowShouldThrow() {
            Row badRow = Row.withNames();
            badRow.setField("location_id", "LOC1");

            assertThatThrownBy(() -> ptf.eval(state, null, badRow))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(collected).isEmpty();
        }

        // --- Helpers ---

        private Row namedAreaRow(String locationId, String areaName, double x1, double y1, double x2, double y2) {
            Row row = Row.withNames();
            row.setField("location_id", locationId);
            row.setField("area_name", areaName);
            row.setField("polygon", new Row[]{
                    point(x1, y1), point(x2, y1),
                    point(x2, y2), point(x1, y2),
                    point(x1, y1)
            });
            return row;
        }

        private Row itemRow(String locationId, String itemId, double x, double y) {
            Row row = Row.withNames();
            row.setField("location_id", locationId);
            row.setField("item_id", itemId);
            row.setField("x", x);
            row.setField("y", y);
            row.setField("lastDetectedTs", 1745000010000L);
            return row;
        }

        private Row point(double x, double y) {
            Row p = Row.withNames();
            p.setField("x", x);
            p.setField("y", y);
            return p;
        }
    }
}
