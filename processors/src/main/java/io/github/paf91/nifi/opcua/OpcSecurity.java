/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MessageSecurityMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.UserTokenType;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.UserTokenPolicy;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Exact endpoint selection shared by both Milo adapters; never downgrades security. */
final class OpcSecurity {
    static final String USERNAME = "Username/Password";
    static final String ANONYMOUS = "Anonymous";
    static final String DEFAULT_APPLICATION_URI = "urn:nifi:opcua:subscriber";
    static final String TCP_PROFILE =
            "http://opcfoundation.org/UA-Profile/Transport/uatcp-uasc-uabinary";
    static final OpcSecurity DEFAULT =
            new OpcSecurity("Basic256Sha256", "SignAndEncrypt", USERNAME);

    private final SecurityPolicy policy;
    private final MessageSecurityMode mode;
    private final UserTokenType identity;
    private final UnencryptedPasswordPolicy unencryptedPasswordPolicy;

    OpcSecurity(String policy, String mode, String authentication) {
        this(policy, mode, authentication, UnencryptedPasswordPolicy.NONE_CHANNELS_ONLY);
    }

    OpcSecurity(
            String policy,
            String mode,
            String authentication,
            UnencryptedPasswordPolicy unencryptedPasswordPolicy) {
        this.policy = SecurityPolicy.valueOf(policy);
        this.mode = MessageSecurityMode.valueOf(mode);
        this.unencryptedPasswordPolicy = Objects.requireNonNull(unencryptedPasswordPolicy);
        if (this.mode == MessageSecurityMode.Invalid
                || (this.policy == SecurityPolicy.None) != (this.mode == MessageSecurityMode.None))
            throw new IllegalArgumentException(
                    "Security Policy None requires Security Mode None; other policies require Sign"
                            + " or SignAndEncrypt");
        if (USERNAME.equals(authentication)) identity = UserTokenType.UserName;
        else if (ANONYMOUS.equals(authentication)) identity = UserTokenType.Anonymous;
        else
            throw new IllegalArgumentException(
                    "Select Anonymous or Username/Password authentication");
    }

    SecurityPolicy policy() {
        return policy;
    }

    MessageSecurityMode mode() {
        return mode;
    }

    boolean usernameAuthentication() {
        return identity == UserTokenType.UserName;
    }

    boolean requiresClientIdentity() {
        return policy != SecurityPolicy.None;
    }

    private Optional<SecurityPolicy> tokenSecurityPolicy(
            EndpointDescription endpoint, UserTokenPolicy token) {
        String uri = token.getSecurityPolicyUri();
        return SecurityPolicy.fromUriSafe(
                uri == null || uri.isEmpty() ? endpoint.getSecurityPolicyUri() : uri);
    }

    UserTokenPolicy userToken(EndpointDescription endpoint) {
        UserTokenPolicy[] tokens = endpoint.getUserIdentityTokens();
        UserTokenPolicy selected = null;
        int selectedRank = Integer.MAX_VALUE;
        boolean rejectedUnencrypted = false;
        if (tokens != null) {
            for (UserTokenPolicy token : tokens) {
                if (token == null || token.getTokenType() != identity) continue;
                if (!usernameAuthentication()) return token;
                Optional<SecurityPolicy> tokenPolicy = tokenSecurityPolicy(endpoint, token);
                if (!tokenPolicy.isPresent() || !supportedTokenPolicy(tokenPolicy.get())) continue;
                int rank;
                if (tokenPolicy.get() == SecurityPolicy.None) {
                    if (!allowsUnencryptedToken(endpoint)) {
                        rejectedUnencrypted = true;
                        continue;
                    }
                    rank = 2;
                } else {
                    rank =
                            tokenPolicy.get().getUri().equals(endpoint.getSecurityPolicyUri())
                                    ? 0
                                    : 1;
                }
                if (rank < selectedRank) {
                    selected = token;
                    selectedRank = rank;
                }
            }
        }
        if (selected != null) return selected;
        if (rejectedUnencrypted)
            throw new IllegalArgumentException(
                    "Allow Unencrypted Password '"
                            + unencryptedPasswordPolicy.value()
                            + "' rejects passwords without token or channel encryption for Security"
                            + " Mode "
                            + endpoint.getSecurityMode()
                            + ". Configure an encrypted username token or SignAndEncrypt,"
                            + " or explicitly set Allow Unencrypted Password to "
                            + (endpoint.getSecurityMode() == MessageSecurityMode.None
                                    ? "None channels only or Always (insecure)"
                                    : "Always (insecure)"));
        throw new IllegalArgumentException(
                "Endpoint does not offer a supported "
                        + (usernameAuthentication() ? USERNAME : ANONYMOUS)
                        + " token");
    }

    private static boolean supportedTokenPolicy(SecurityPolicy tokenPolicy) {
        switch (tokenPolicy) {
            case None:
            case Basic128Rsa15:
            case Basic256:
            case Basic256Sha256:
            case Aes128_Sha256_RsaOaep:
            case Aes256_Sha256_RsaPss:
                return true;
            default:
                return false;
        }
    }

    private boolean allowsUnencryptedToken(EndpointDescription endpoint) {
        return endpoint.getSecurityMode() == MessageSecurityMode.SignAndEncrypt
                || unencryptedPasswordPolicy == UnencryptedPasswordPolicy.ALWAYS
                || (unencryptedPasswordPolicy == UnencryptedPasswordPolicy.NONE_CHANNELS_ONLY
                        && endpoint.getSecurityMode() == MessageSecurityMode.None);
    }

    private boolean matchesChannel(EndpointDescription endpoint) {
        if (endpoint == null
                || !policy.getUri().equals(endpoint.getSecurityPolicyUri())
                || mode != endpoint.getSecurityMode()
                || !TCP_PROFILE.equals(endpoint.getTransportProfileUri())) return false;
        try {
            URI uri = URI.create(endpoint.getEndpointUrl());
            return "opc.tcp".equals(uri.getScheme()) && uri.getHost() != null;
        } catch (RuntimeException e) {
            return false;
        }
    }

    boolean matches(EndpointDescription endpoint) {
        if (!matchesChannel(endpoint)) return false;
        try {
            userToken(endpoint);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    EndpointDescription selectEndpoint(List<EndpointDescription> endpoints) {
        IllegalArgumentException tokenError = null;
        for (EndpointDescription endpoint : endpoints) {
            if (!matchesChannel(endpoint)) continue;
            try {
                userToken(endpoint);
                return endpoint;
            } catch (IllegalArgumentException e) {
                if (tokenError == null) tokenError = e;
            }
        }
        if (tokenError != null) throw tokenError;
        throw new IllegalArgumentException(
                "No OPC TCP endpoint for "
                        + policy.name()
                        + " / "
                        + mode.name()
                        + " / "
                        + (usernameAuthentication() ? USERNAME : ANONYMOUS));
    }

    boolean requiresServerTrust(EndpointDescription endpoint) {
        return requiresClientIdentity()
                || (usernameAuthentication()
                        && tokenSecurityPolicy(endpoint, userToken(endpoint))
                                        .orElseThrow(
                                                () ->
                                                        new IllegalArgumentException(
                                                                "Unsupported username token"
                                                                        + " security policy"))
                                != SecurityPolicy.None);
    }
}
