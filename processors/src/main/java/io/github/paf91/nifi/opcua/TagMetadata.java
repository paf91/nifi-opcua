/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import com.fasterxml.jackson.core.JsonGenerator;

import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.structured.EUInformation;

import java.io.IOException;

/** Immutable, bounded metadata snapshot. Missing or oversized metadata is explicitly null. */
final class TagMetadata {
    static final TagMetadata EMPTY = new TagMetadata(null, (EUInformation) null);
    static final int MAX_TEXT = 1024;
    final String dataType;
    private final boolean unitsPresent;
    private final String namespaceUri, displayName, description;
    private final Integer unitId;

    TagMetadata(String dataType, EUInformation units) {
        this.dataType = bounded(dataType) ? dataType : null;
        unitsPresent =
                units != null
                        && bounded(units.getNamespaceUri())
                        && bounded(text(units.getDisplayName()))
                        && bounded(text(units.getDescription()));
        namespaceUri = unitsPresent ? units.getNamespaceUri() : null;
        displayName = unitsPresent ? text(units.getDisplayName()) : null;
        description = unitsPresent ? text(units.getDescription()) : null;
        unitId = unitsPresent ? units.getUnitId() : null;
    }

    TagMetadata(String dataType, TagMetadata other) {
        this.dataType = dataType;
        unitsPresent = other.unitsPresent;
        namespaceUri = other.namespaceUri;
        displayName = other.displayName;
        description = other.description;
        unitId = other.unitId;
    }

    private static boolean bounded(String value) {
        return value == null || value.length() <= MAX_TEXT;
    }

    private static String text(LocalizedText value) {
        return value == null ? null : value.getText();
    }

    boolean hasUnits() {
        return unitsPresent;
    }

    void writeUnits(JsonGenerator generator) throws IOException {
        generator.writeFieldName("engineeringUnits");
        if (!unitsPresent) {
            generator.writeNull();
            return;
        }
        generator.writeStartObject();
        generator.writeStringField("namespaceUri", namespaceUri);
        if (unitId == null) generator.writeNullField("unitId");
        else generator.writeNumberField("unitId", unitId);
        generator.writeStringField("displayName", displayName);
        generator.writeStringField("description", description);
        generator.writeEndObject();
    }
}
