package io.confluent.example.geofencing.maps;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.locationtech.jts.geom.GeometryFactory;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GeoLocatorIntegrationTest {

    private List<NamedArea> areas;
    private final GeoLocator geoLocator = new GeoLocator(new GeometryFactory());

    @BeforeAll
    void loadMap() throws IOException {
        areas = new LocationLoader().loadMapFromResource("maps/ES_0279.dxf");
    }

    @Nested
    class Stockroom1 {

        @Test
        void centerOfStockroom1() {
            List<String> matched = geoLocator.locateAreas(areas, 2050, 25300);
            assertThat(matched).singleElement().asString().startsWith("STOCKROOM 1");
        }

        @Test
        void topOfStore() {
            List<String> matched = geoLocator.locateAreas(areas, 3000, 30000);
            assertThat(matched).singleElement().asString().startsWith("STOCKROOM 1");
        }

        @Test
        void leftSideUpperFloor() {
            List<String> matched = geoLocator.locateAreas(areas, 1000, 25000);
            assertThat(matched).singleElement().asString().startsWith("STOCKROOM 1");
        }
    }

    @Nested
    class Frontstore {

        @Test
        void centerOfFrontstore() {
            List<String> matched = geoLocator.locateAreas(areas, 5300, 22000);
            assertThat(matched).singleElement().asString().startsWith("FRONTSTORE");
        }

        @Test
        void rightSideUpperFloor() {
            List<String> matched = geoLocator.locateAreas(areas, 7000, 25000);
            assertThat(matched).singleElement().asString().startsWith("FRONTSTORE");
        }
    }

    @Nested
    class Stockroom2 {

        @Test
        void centerOfStockroom2() {
            List<String> matched = geoLocator.locateAreas(areas, 4050, 11250);
            assertThat(matched).singleElement().asString().startsWith("STOCKROOM 2");
        }
    }

    @Nested
    class Stockroom3 {

        @Test
        void centerOfStockroom3() {
            List<String> matched = geoLocator.locateAreas(areas, 3840, 3070);
            assertThat(matched).singleElement().asString().startsWith("STOCKROOM 3");
        }
    }

    @Nested
    class OutsideAllAreas {

        @Test
        void gapBetweenFrontstoreAndStockroom2() {
            List<String> matched = geoLocator.locateAreas(areas, 7000, 15000);
            assertThat(matched).isEmpty();
        }

        @Test
        void gapBetweenStockroom1AndFrontstore() {
            List<String> matched = geoLocator.locateAreas(areas, 2500, 20000);
            assertThat(matched).isEmpty();
        }

        @Test
        void farOutsideBuilding() {
            List<String> matched = geoLocator.locateAreas(areas, 50000, 50000);
            assertThat(matched).isEmpty();
        }

        @Test
        void negativeCoordinates() {
            List<String> matched = geoLocator.locateAreas(areas, -5000, -5000);
            assertThat(matched).isEmpty();
        }
    }
}
