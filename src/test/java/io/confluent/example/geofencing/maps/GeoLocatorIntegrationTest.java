package io.confluent.example.geofencing.maps;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GeoLocatorIntegrationTest {

    private Location map;
    private final GeoLocator geoLocator = new GeoLocator();

    @BeforeAll
    void loadMap() throws IOException {
        map = new LocationLoader().loadMapFromResource("maps/ES_0279.dxf");
    }

    @Nested
    class Stockroom1 {

        @Test
        void centerOfStockroom1() {
            List<String> areas = geoLocator.locateAreas(map, 2050, 25300);
            assertThat(areas).singleElement().asString().startsWith("STOCKROOM 1");
        }

        @Test
        void topOfStore() {
            List<String> areas = geoLocator.locateAreas(map, 3000, 30000);
            assertThat(areas).singleElement().asString().startsWith("STOCKROOM 1");
        }

        @Test
        void leftSideUpperFloor() {
            List<String> areas = geoLocator.locateAreas(map, 1000, 25000);
            assertThat(areas).singleElement().asString().startsWith("STOCKROOM 1");
        }
    }

    @Nested
    class Frontstore {

        @Test
        void centerOfFrontstore() {
            List<String> areas = geoLocator.locateAreas(map, 5300, 22000);
            assertThat(areas).singleElement().asString().startsWith("FRONTSTORE");
        }

        @Test
        void rightSideUpperFloor() {
            List<String> areas = geoLocator.locateAreas(map, 7000, 25000);
            assertThat(areas).singleElement().asString().startsWith("FRONTSTORE");
        }
    }

    @Nested
    class Stockroom2 {

        @Test
        void centerOfStockroom2() {
            List<String> areas = geoLocator.locateAreas(map, 4050, 11250);
            assertThat(areas).singleElement().asString().startsWith("STOCKROOM 2");
        }
    }

    @Nested
    class Stockroom3 {

        @Test
        void centerOfStockroom3() {
            List<String> areas = geoLocator.locateAreas(map, 3840, 3070);
            assertThat(areas).singleElement().asString().startsWith("STOCKROOM 3");
        }
    }

    @Nested
    class OutsideAllAreas {

        @Test
        void gapBetweenFrontstoreAndStockroom2() {
            List<String> areas = geoLocator.locateAreas(map, 7000, 15000);
            assertThat(areas).isEmpty();
        }

        @Test
        void gapBetweenStockroom1AndFrontstore() {
            List<String> areas = geoLocator.locateAreas(map, 2500, 20000);
            assertThat(areas).isEmpty();
        }

        @Test
        void farOutsideBuilding() {
            List<String> areas = geoLocator.locateAreas(map, 50000, 50000);
            assertThat(areas).isEmpty();
        }

        @Test
        void negativeCoordinates() {
            List<String> areas = geoLocator.locateAreas(map, -5000, -5000);
            assertThat(areas).isEmpty();
        }
    }
}
