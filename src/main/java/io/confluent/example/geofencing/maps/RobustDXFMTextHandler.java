package io.confluent.example.geofencing.maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kabeja.dxf.DXFDocument;
import org.kabeja.dxf.DXFEntity;
import org.kabeja.parser.DXFValue;
import org.kabeja.parser.entities.DXFEntityHandler;
import org.kabeja.parser.entities.DXFMTextHandler;

import java.lang.reflect.Field;

/**
 * Wraps Kabeja's DXFMTextHandler to catch RuntimeExceptions thrown
 * when parsing malformed MTEXT formatting codes.
 * Problematic MTEXT entities are skipped; valid ones pass through normally.
 */
class RobustDXFMTextHandler implements DXFEntityHandler {

    private static final Logger LOGGER = LogManager.getLogger(RobustDXFMTextHandler.class);

    private final DXFMTextHandler delegate = new DXFMTextHandler();

    @Override
    public String getDXFEntityName() {
        return delegate.getDXFEntityName();
    }

    @Override
    public void setDXFDocument(DXFDocument doc) {
        delegate.setDXFDocument(doc);
    }

    @Override
    public void releaseDXFDocument() {
        delegate.releaseDXFDocument();
    }

    @Override
    public void startDXFEntity() {
        delegate.startDXFEntity();
    }

    @Override
    public void parseGroup(int groupCode, DXFValue value) {
        delegate.parseGroup(groupCode, value);
    }

    @Override
    public DXFEntity getDXFEntity() {
        return delegate.getDXFEntity();
    }

    @Override
    public void endDXFEntity() {
        try {
            delegate.endDXFEntity();
        } catch (RuntimeException e) {
            LOGGER.warn("Skipping malformed MTEXT entity: {}", e.getMessage());
            // DXFMTextHandler.endDXFEntity() clears its internal StringBuffer after
            // calling setText(). When setText() throws, the buffer retains accumulated
            // text which leaks into subsequent entities. Clear it via reflection.
            clearDelegateBuffer();
        }
    }

    private void clearDelegateBuffer() {
        try {
            Field bufField = DXFMTextHandler.class.getDeclaredField("buf");
            bufField.setAccessible(true);
            StringBuffer buf = (StringBuffer) bufField.get(delegate);
            buf.delete(0, buf.length());
        } catch (ReflectiveOperationException e) {
            LOGGER.warn("Could not clear MTEXT handler buffer: {}", e.getMessage());
        }
    }

    @Override
    public boolean isFollowSequence() {
        return delegate.isFollowSequence();
    }
}
