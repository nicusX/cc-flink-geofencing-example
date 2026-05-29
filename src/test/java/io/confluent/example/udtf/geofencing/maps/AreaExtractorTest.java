package io.confluent.example.udtf.geofencing.maps;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.kabeja.dxf.DXFLWPolyline;
import org.kabeja.dxf.DXFVertex;
import org.kabeja.dxf.helpers.Point;
import org.locationtech.jts.geom.Polygon;

import static org.assertj.core.api.Assertions.assertThat;

class AreaExtractorTest {

    private final AreaExtractor extractor = new AreaExtractor();

    @Nested
    class CleanDxfText {

        @Test
        void shouldReturnEmptyForNull() {
            assertThat(extractor.cleanDxfText(null)).isEmpty();
        }

        @Test
        void shouldReturnEmptyForEmpty() {
            assertThat(extractor.cleanDxfText("")).isEmpty();
        }

        @Test
        void shouldPreservePlainText() {
            assertThat(extractor.cleanDxfText("B-1")).isEqualTo("B-1");
        }

        @Test
        void shouldStripFontFormatting() {
            assertThat(extractor.cleanDxfText("\\fArial|b0|i0|c0|p34;B-1"))
                    .isEqualTo("B-1");
        }

        @Test
        void shouldStripMultipleFormattingBlocks() {
            assertThat(extractor.cleanDxfText("\\fArial|b0;\\A1;Zone A"))
                    .isEqualTo("Zone A");
        }

        @Test
        void shouldStripParagraphMarkers() {
            assertThat(extractor.cleanDxfText("Line1\\PLine2"))
                    .isEqualTo("Line1Line2");
        }

        @Test
        void shouldStripLowercaseParagraphMarkers() {
            assertThat(extractor.cleanDxfText("Line1\\pLine2"))
                    .isEqualTo("Line1Line2");
        }

        @Test
        void shouldTrimWhitespace() {
            assertThat(extractor.cleanDxfText("  B-1  ")).isEqualTo("B-1");
        }

        @Test
        void shouldHandleCombinedFormatting() {
            assertThat(extractor.cleanDxfText("\\fArial|b0|i0|c0|p34;\\A1;B-1\\PB-2"))
                    .isEqualTo("B-1B-2");
        }
    }

    @Nested
    class CreatePolygonFromDXF {

        @Test
        void shouldReturnNullForFewerThanThreeVertices() {
            DXFLWPolyline pline = lwPolyline(
                    new double[]{0, 0}, new double[]{1, 1}
            );
            assertThat(extractor.createPolygonFromDXF(pline)).isNull();
        }

        @Test
        void shouldCreateValidPolygonFromTriangle() {
            DXFLWPolyline pline = lwPolyline(
                    new double[]{0, 0}, new double[]{10, 0}, new double[]{5, 10}
            );
            Polygon poly = extractor.createPolygonFromDXF(pline);
            assertThat(poly).isNotNull();
            assertThat(poly.isValid()).isTrue();
            assertThat(poly.getCoordinates()).hasSize(4);
        }

        @Test
        void shouldClosePolygonAutomatically() {
            DXFLWPolyline pline = lwPolyline(
                    new double[]{0, 0}, new double[]{10, 0}, new double[]{10, 10}, new double[]{0, 10}
            );
            Polygon poly = extractor.createPolygonFromDXF(pline);
            assertThat(poly).isNotNull();
            var coords = poly.getCoordinates();
            assertThat(coords[0]).isEqualTo(coords[coords.length - 1]);
        }

        @Test
        void shouldCreateValidPolygonFromRectangle() {
            DXFLWPolyline pline = lwPolyline(
                    new double[]{0, 0}, new double[]{100, 0}, new double[]{100, 50}, new double[]{0, 50}
            );
            Polygon poly = extractor.createPolygonFromDXF(pline);
            assertThat(poly).isNotNull();
            assertThat(poly.isValid()).isTrue();
            assertThat(poly.getArea()).isEqualTo(5000.0);
        }

        private DXFLWPolyline lwPolyline(double[]... points) {
            DXFLWPolyline pline = new DXFLWPolyline();
            for (double[] pt : points) {
                DXFVertex v = new DXFVertex();
                v.setPoint(new Point(pt[0], pt[1], 0));
                pline.addVertex(v);
            }
            return pline;
        }
    }
}
