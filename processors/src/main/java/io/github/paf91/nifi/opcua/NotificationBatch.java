/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;

import java.util.*;

/** The same content, attributes and provenance for scheduled and stop-time delivery. */
final class NotificationBatch {
    static void write(
            ProcessSession session,
            List<byte[]> records,
            String endpoint,
            String securityPolicy,
            String securityMode) {
        write(
                session,
                records,
                endpoint,
                securityPolicy,
                securityMode,
                OutputSettings.DEFAULT,
                ConsumeOPCUA.SUCCESS);
    }

    static void write(
            ProcessSession session,
            List<byte[]> records,
            String endpoint,
            String securityPolicy,
            String securityMode,
            OutputSettings output,
            Relationship relationship) {
        FlowFile flow = session.create();
        flow =
                session.write(
                        flow,
                        out -> {
                            if (!output.ndjson) out.write('[');
                            for (int i = 0; i < records.size(); i++) {
                                if (i > 0 && !output.ndjson) out.write(',');
                                out.write(records.get(i));
                                if (output.ndjson) out.write('\n');
                            }
                            if (!output.ndjson) out.write(']');
                        });
        Map<String, String> attributes = new HashMap<>();
        attributes.put("mime.type", output.ndjson ? "application/x-ndjson" : "application/json");
        attributes.put("record.count", Integer.toString(records.size()));
        attributes.put("opcua.endpoint", endpoint);
        attributes.put("opcua.security.policy", securityPolicy);
        attributes.put("opcua.security.mode", securityMode);
        attributes.put("opcua.output.fields", output.fieldNames);
        attributes.put("opcua.output.format", output.ndjson ? "NDJSON" : "JSON Array");
        attributes.put(
                "opcua.timestamp.format", output.epochMillis ? "Epoch Milliseconds" : "ISO-8601");
        if (output.routeQuality) attributes.put("opcua.quality.route", relationship.getName());
        flow = session.putAllAttributes(flow, attributes);
        session.getProvenanceReporter().receive(flow, endpoint);
        session.transfer(flow, relationship);
    }
}
