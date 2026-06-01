package io.confluent.example.geofencing.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.example.geofencing.maps.LocationLoader;
import io.confluent.example.geofencing.maps.NamedArea;
import org.locationtech.jts.geom.Coordinate;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

/**
 * Command line utility to parse a DXF file defining the maps of a specific location ang generating the records
 * for the named_area_maps table, either as INSERT INTO statements or JSON.
 *
 * The location ID is implicitly the name of the .dxf file (without the .dxf extension)
 */
public class Dxf2NamedAreaMapsCLI {

    public static void main(String[] args) throws Exception {
        if (args.length != 2 || (!args[1].equals("sql") && !args[1].equals("json"))) {
            System.err.println("Usage: Dxf2NamedAreaMapsCLI <dxf-file> <sql|json>");
            System.exit(1);
        }

        String filePath = args[0];
        String format = args[1];

        Path path = Path.of(filePath);
        String filename = path.getFileName().toString();
        if (!filename.endsWith(".dxf")) {
            System.err.println("File must have .dxf extension: " + filename);
            System.exit(1);
        }
        String locationId = filename.substring(0, filename.length() - 4);

        LocationLoader loader = new LocationLoader();
        List<NamedArea> areas;
        try (InputStream in = new FileInputStream(path.toFile())) {
            areas = loader.loadMap(in, filePath);
        }

        if (areas.isEmpty()) {
            System.err.println("No areas found in " + filePath);
            return;
        }

        switch (format) {
            case "sql" -> printSql(locationId, areas);
            case "json" -> printJson(locationId, areas);
        }
    }

    private static void printSql(String locationId, List<NamedArea> areas) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO named_area_maps (location_id, area_name, polygon)\nVALUES\n");

        for (int i = 0; i < areas.size(); i++) {
            NamedArea area = areas.get(i);
            Coordinate[] coords = area.polygon.getCoordinates();

            sb.append("  ('").append(escapeSql(locationId)).append("', '")
              .append(escapeSql(area.name)).append("', ARRAY[");

            for (int j = 0; j < coords.length; j++) {
                if (j > 0) sb.append(", ");
                sb.append("ROW(").append(coords[j].x).append(", ").append(coords[j].y).append(")");
            }

            sb.append("])");
            sb.append(i < areas.size() - 1 ? ",\n" : ";\n");
        }

        System.out.print(sb);
    }

    private static void printJson(String locationId, List<NamedArea> areas) throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        for (NamedArea area : areas) {
            ObjectNode node = mapper.createObjectNode();
            node.put("location_id", locationId);
            node.put("area_name", area.name);

            ArrayNode polygonArray = node.putArray("polygon");
            for (Coordinate coord : area.polygon.getCoordinates()) {
                ObjectNode vertex = polygonArray.addObject();
                vertex.put("x", coord.x);
                vertex.put("y", coord.y);
            }

            System.out.println(mapper.writeValueAsString(node));
        }
    }

    private static String escapeSql(String value) {
        return value.replace("'", "''");
    }
}
