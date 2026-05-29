package io.confluent.example.udtf.geofencing.maps;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocationLoaderTest {

    private final LocationLoader loader = new LocationLoader();

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class LoadMapFromResource {

        private Location result;

        @BeforeAll
        void loadOnce() throws IOException {
            result = loader.loadMapFromResource("maps/ES_0279.dxf");
        }

        @Test
        void shouldReturnCorrectLocationId() {
            assertThat(result.locationId).isEqualTo("ES_0279");
        }

        @Test
        void shouldExtractAreas() {
            assertThat(result.areas).isNotEmpty();
        }

        @Test
        void allAreaNamesShouldNotBeBlank() {
            assertThat(result.areas)
                    .allSatisfy(area -> assertThat(area.name).isNotBlank());
        }

        @Test
        void allPolygonsShouldBeValid() {
            assertThat(result.areas)
                    .allSatisfy(area -> {
                        assertThat(area.polygon).isNotNull();
                        assertThat(area.polygon.isValid()).isTrue();
                    });
        }

        @Test
        void allPolygonsShouldBeClosed() {
            assertThat(result.areas)
                    .allSatisfy(area -> {
                        var coords = area.polygon.getCoordinates();
                        assertThat(coords.length).isGreaterThanOrEqualTo(4);
                        assertThat(coords[0]).isEqualTo(coords[coords.length - 1]);
                    });
        }
    }

    @Nested
    class ListDxfResourcePaths {

        @Test
        void shouldFindResourcePaths() throws IOException {
            List<String> paths = loader.listDxfResourcePaths();
            assertThat(paths).contains("maps/ES_0279.dxf");
        }

        @Test
        void shouldReturnSortedList() throws IOException {
            List<String> paths = loader.listDxfResourcePaths();
            assertThat(paths).isSorted();
        }

        @Test
        void allPathsShouldEndWithDxf() throws IOException {
            List<String> paths = loader.listDxfResourcePaths();
            assertThat(paths).allSatisfy(p -> assertThat(p).endsWith(".dxf"));
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class LoadAllMapsFromResources {

        private Map<String, Location> results;

        @BeforeAll
        void loadOnce() throws IOException {
            results = loader.loadAllMapsFromResources();
        }

        @Test
        void shouldLoadAtLeastOneMap() {
            assertThat(results).isNotEmpty();
        }

        @Test
        void shouldContainES0279() {
            assertThat(results).containsKey("ES_0279");
        }

        @Test
        void allMapsShouldHaveAreas() {
            assertThat(results.values())
                    .allSatisfy(map -> assertThat(map.areas).isNotEmpty());
        }
    }

    @Test
    void loadMapFromResource_shouldThrowForMissingResource() {
        assertThatThrownBy(() -> loader.loadMapFromResource("maps/NONEXISTENT.dxf"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("NONEXISTENT");
    }
}
