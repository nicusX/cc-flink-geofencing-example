package io.confluent.example.geofencing.maps;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kabeja.dxf.DXFDocument;
import org.kabeja.parser.DXFParser;
import org.kabeja.parser.DXFEntitiesSectionHandler;
import org.kabeja.parser.DXFSectionHandler;
import org.kabeja.parser.ParseException;
import org.kabeja.parser.Parser;
import org.kabeja.parser.ParserBuilder;

/**
 * Loads and parses a DXF map resource for a location.
 *
 * Must be initialized lazily and not put as a property of the UDF class - it's not serializable
 */
public class LocationLoader {
    private static final Logger LOGGER = LogManager.getLogger(LocationLoader.class);

    /**
     * Resource folder containing the dxf maps
     */
    public static final String MAP_RESOURCE_FOLDER = "maps";

    private final AreaExtractor areaExtractor;

    public LocationLoader() {
        areaExtractor = new AreaExtractor();
    }

    private static String locationIdFromPath(String resourcePath) {
        String filename = resourcePath.substring(resourcePath.lastIndexOf('/') + 1);
        return filename.substring(0, filename.length() - 4);
    }

    private InputStream inputFromResource(String resourcePath) {
        InputStream is = LocationLoader.class.getClassLoader().getResourceAsStream(resourcePath);
        if (is == null) {
            LOGGER.error("Map resource file {} not found", resourcePath);
            throw new IllegalArgumentException("Resource file " + resourcePath + " not found");
        }
        return is;
    }

    /**
     * Parse a DXF document from an input stream
     *
     * @param mapStream InputStream of the dxf
     * @return a DXFDocument with the map definition
     * @throws ParseException if anything goes wrong
     */
    private DXFDocument parseDxfDocument(InputStream mapStream) throws ParseException {
        Parser parser = ParserBuilder.createDefaultParser();
        installRobustMTextHandler(parser);
        parser.parse(mapStream, DXFParser.DEFAULT_ENCODING);
        return parser.getDocument();
    }

    /**
     * Replaces the default MTEXT handler with a robust wrapper that catches
     * RuntimeExceptions from malformed MTEXT formatting codes.
     */
    @SuppressWarnings("unchecked")
    private void installRobustMTextHandler(Parser parser) {
        try {
            RobustDXFMTextHandler robustHandler = new RobustDXFMTextHandler();
            Field sectionHandlersField = DXFParser.class.getDeclaredField("handlers");
            sectionHandlersField.setAccessible(true);
            Hashtable<String, DXFSectionHandler> sectionHandlers =
                    (Hashtable<String, DXFSectionHandler>) sectionHandlersField.get(parser);

            for (DXFSectionHandler sectionHandler : sectionHandlers.values()) {
                if (sectionHandler instanceof DXFEntitiesSectionHandler entitySectionHandler) {
                    entitySectionHandler.addDXFEntityHandler(robustHandler);
                }
            }
        } catch (ReflectiveOperationException e) {
            LOGGER.warn("Could not install robust MTEXT handler, parsing may fail on malformed MTEXT: {}", e.getMessage());
        }
    }


    /**
     * Lists all DXF resource paths by scanning the maps resource folder.
     * Handles both filesystem (dev/test) and JAR (production) classpath layouts.
     */
    List<String> listDxfResourcePaths() throws IOException {
        URL dirUrl = LocationLoader.class.getClassLoader().getResource(MAP_RESOURCE_FOLDER);
        if (dirUrl == null) {
            throw new IllegalStateException(
                    "Resource folder '" + MAP_RESOURCE_FOLDER + "' not found on classpath");
        }

        String prefix = MAP_RESOURCE_FOLDER + "/";
        List<String> resourcePaths = new ArrayList<>();

        if ("file".equals(dirUrl.getProtocol())) {
            File dir = new File(dirUrl.getPath());
            String[] files = dir.list();
            if (files != null) {
                for (String file : files) {
                    if (file.endsWith(".dxf")) {
                        resourcePaths.add(prefix + file);
                    }
                }
            }
        } else if ("jar".equals(dirUrl.getProtocol())) {
            JarURLConnection jarConn = (JarURLConnection) dirUrl.openConnection();
            try (JarFile jar = jarConn.getJarFile()) {
                Enumeration<JarEntry> entries = jar.entries();
                while (entries.hasMoreElements()) {
                    String name = entries.nextElement().getName();
                    if (name.startsWith(prefix) && name.endsWith(".dxf") && !name.substring(prefix.length()).contains("/")) {
                        resourcePaths.add(name);
                    }
                }
            }
        }

        resourcePaths.sort(String::compareTo);
        return resourcePaths;
    }

    /**
     * Reads a DXF resource, parses it, and generates the location map.
     *
     * @param resourcePath resource path of the .dxf file (e.g. "maps/ES_0279.dxf")
     * @return Location with all the areas defined by the map
     */
    public Location loadMapFromResource(String resourcePath) throws IOException {
        String locationId = locationIdFromPath(resourcePath);
        LOGGER.info("Parsing map of location: {}", locationId);

        try (InputStream mapStream = inputFromResource(resourcePath)) {
            DXFDocument dxfDoc = parseDxfDocument(mapStream);
            List<NamedArea> areas = areaExtractor.extractAreas(dxfDoc);

            LOGGER.info("Map of location {} successfully parsed. {} areas defined", locationId, areas.size());
            return new Location(locationId, areas);
        } catch (IOException ioe) {
            LOGGER.error("Exception parsing the map " + resourcePath, ioe);
            throw ioe;
        } catch (ParseException pe) {
            LOGGER.error("Exception parsing the map file " + resourcePath, pe);
            throw new RuntimeException("Exception parsing the map file " + resourcePath, pe);
        }
    }


    /**
     * Discovers all DXF map files in the resource folder and loads each one.
     *
     * @return map of locationId to Location for all maps found in the resource folder
     */
    public Map<String, Location> loadAllMapsFromResources() throws IOException {
        List<String> resourcePaths = listDxfResourcePaths();
        LOGGER.info("Found {} map resources in: {}", resourcePaths.size(), resourcePaths);
        Map<String, Location> maps = new HashMap<>();
        for (String resourcePath : resourcePaths) {
            Location location = loadMapFromResource(resourcePath);
            maps.put(location.locationId, location);
        }
        return maps;
    }
}
