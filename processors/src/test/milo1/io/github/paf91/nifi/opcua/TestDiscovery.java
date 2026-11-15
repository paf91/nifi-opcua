/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import org.eclipse.milo.opcua.sdk.client.DiscoveryClient;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;

import java.util.List;
import java.util.concurrent.TimeUnit;

final class TestDiscovery {
    static List<EndpointDescription> getEndpoints(String url) throws Exception {
        return DiscoveryClient.getEndpoints(url).get(5, TimeUnit.SECONDS);
    }
}
