package io.confluent.example.geofencing.maps;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Hashtable;
import java.util.List;

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
 * Loads and parses a DXF map for a location from an input stream.
 *
 * Must be initialized lazily and not put as a property of the UDF class - it's not serializable
 */
public class LocationLoader {
    private static final Logger LOGGER = LogManager.getLogger(LocationLoader.class);

    private final AreaExtractor areaExtractor;

    public LocationLoader() {
        areaExtractor = new AreaExtractor();
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
     * Parses a DXF map from an arbitrary InputStream.
     *
     * @param dxfStream  InputStream of the DXF file (caller is responsible for closing)
     * @param sourceName descriptive name used in log messages (e.g. file path or resource path)
     * @return list of named areas extracted from the DXF document
     */
    public List<NamedArea> loadMap(InputStream dxfStream, String sourceName) throws IOException {
        LOGGER.info("Parsing map: {}", sourceName);

        try {
            DXFDocument dxfDoc = parseDxfDocument(dxfStream);
            List<NamedArea> areas = areaExtractor.extractAreas(dxfDoc);

            LOGGER.info("Map {} successfully parsed. {} areas defined", sourceName, areas.size());
            return areas;
        } catch (ParseException pe) {
            LOGGER.error("Exception parsing the map file " + sourceName, pe);
            throw new RuntimeException("Exception parsing the map file " + sourceName, pe);
        }
    }
}
