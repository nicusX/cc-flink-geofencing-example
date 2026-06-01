package io.confluent.example.geofencing.ptf;

import io.confluent.example.geofencing.maps.NamedArea;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GeoLocatorDynamicMapsPTFTest {

    private final GeoLocatorDynamicMapsPTF ptf = new GeoLocatorDynamicMapsPTF();

    @BeforeEach
    void setUp() throws Exception {
        ptf.open(null);
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
}
