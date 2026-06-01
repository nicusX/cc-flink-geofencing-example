package io.confluent.example.geofencing.maps;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.kabeja.dxf.*;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/**
 * Extracts named areas from a parsed DXF document by matching
 * boundary polygons (from A-AREA-BNDY layers) with text labels
 * (from layers containing a configurable identifier string).
 */
public class AreaExtractor {

    /**
     * In the DXF file, the layer defining the areas must contain this string
     */
    public static final String DXF_LAYER_DEFINING_AREAS_MUST_CONTAIN = "IDEN";

    private final GeometryFactory geometryFactory;

    public AreaExtractor() {
        geometryFactory = new GeometryFactory();
    }

    private static class TextLabel {
        String text;
        Point location;

        TextLabel(String text, Point location) {
            this.text = text;
            this.location = location;
        }
    }

    public List<NamedArea> extractAreas(DXFDocument doc) {
        List<Polygon> boundaryPolygons = new ArrayList<>();
        List<TextLabel> labels = new ArrayList<>();
        List<NamedArea> finalAreas = new ArrayList<>();

        // Iterate through all layers in the DXF document
        Iterator<?> layerIt = doc.getDXFLayerIterator();
        while (layerIt.hasNext()) {
            DXFLayer layer = (DXFLayer) layerIt.next();
            String layerName = layer.getName().toUpperCase();

            // === EXTRACT BOUNDARIES ===
            // Based on your DXF, area outlines are in A-AREA-BNDY layers
            if (layerName.contains("A-AREA-BNDY")) {

                // Get all LWPolylines (LightWeight Polylines) from this layer
                List<?> lwPolylines = layer.getDXFEntities(DXFConstants.ENTITY_TYPE_LWPOLYLINE);
                if (lwPolylines != null) {
                    for (Object obj : lwPolylines) {
                        DXFLWPolyline pline = (DXFLWPolyline) obj;
                        Polygon poly = createPolygonFromDXF(pline);
                        if (poly != null) {
                            boundaryPolygons.add(poly);
                        }
                    }
                }
            }

            // === EXTRACT LABELS ===
            // Only layers containing the specified string in the name are considered for Areas
            if (layerName.contains(DXF_LAYER_DEFINING_AREAS_MUST_CONTAIN)) {

                // Get MTEXT (Multi-line text) entities
                List<?> mtexts = layer.getDXFEntities(DXFConstants.ENTITY_TYPE_MTEXT);
                if (mtexts != null) {
                    for (Object obj : mtexts) {
                        DXFMText mtext = (DXFMText) obj;
                        double x = mtext.getInsertPoint().getX();
                        double y = mtext.getInsertPoint().getY();

                        Point pt = geometryFactory.createPoint(new Coordinate(x, y));
                        labels.add(new TextLabel(cleanDxfText(mtext.getText()), pt));
                    }
                }

                // Also check standard TEXT entities just in case
                List<?> texts = layer.getDXFEntities(DXFConstants.ENTITY_TYPE_TEXT);
                if (texts != null) {
                    for (Object obj : texts) {
                        DXFText text = (DXFText) obj;
                        double x = text.getInsertPoint().getX();
                        double y = text.getInsertPoint().getY();

                        Point pt = geometryFactory.createPoint(new Coordinate(x, y));
                        labels.add(new TextLabel(cleanDxfText(text.getText()), pt));
                    }
                }
            }
        }

        // Spatial Join: Match labels to polygons
        for (Polygon poly : boundaryPolygons) {
            for (TextLabel label : labels) {
                // If the Text's insertion point falls inside the Polygon's boundaries
                if (poly.contains(label.location)) {
                    finalAreas.add(new NamedArea(label.text, poly));
                    break; // Move to the next polygon once a match is found
                }
            }
        }

        return finalAreas;
    }

    /**
     * DXF MText often contains formatting codes like \A1; \P or font definitions.
     * This strips them out to leave just "B-1", "B-2", etc.
     */
    String cleanDxfText(String rawText) {
        if (rawText == null) return "";
        // Removes DXF formatting blocks like \fArial|b0|i0|c0|p34;
        String cleaned = rawText.replaceAll("\\\\.*?;", "");
        // Removes newlines or paragraph markers
        cleaned = cleaned.replaceAll("\\\\[pP]", "");
        return cleaned.trim();
    }

    /**
     * Converts a Kabeja DXFLWPolyline into a JTS Polygon.
     */
    Polygon createPolygonFromDXF(DXFLWPolyline pline) {
        if (pline.getVertexCount() < 3) return null;

        List<Coordinate> coords = new ArrayList<>();
        for (int i = 0; i < pline.getVertexCount(); i++) {
            DXFVertex v = pline.getVertex(i);
            coords.add(new Coordinate(v.getX(), v.getY()));
        }

        // JTS requires Polygons to be explicitly closed (first and last coordinate must be identical).
        Coordinate firstPoint = coords.get(0);
        Coordinate lastPoint = coords.get(coords.size() - 1);

        if (!firstPoint.equals2D(lastPoint)) {
            coords.add(new Coordinate(firstPoint.x, firstPoint.y));
        }

        // Must have at least 4 points to form a closed linear ring in JTS (e.g., Triangle = 3 points + 1 closing point)
        if (coords.size() < 4) return null;

        return geometryFactory.createPolygon(coords.toArray(new Coordinate[0]));
    }
}
