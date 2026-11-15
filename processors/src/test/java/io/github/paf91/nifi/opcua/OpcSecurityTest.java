/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.ubyte;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.nifi.util.TestRunner;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.enumerated.*;
import org.eclipse.milo.opcua.stack.core.types.structured.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.stream.Stream;

class OpcSecurityTest {
    static Stream<Object[]> configurations() {
        List<Object[]> cases = new ArrayList<>();
        for (String policy :
                Arrays.asList(
                        "None",
                        "Basic128Rsa15",
                        "Basic256",
                        "Basic256Sha256",
                        "Aes128_Sha256_RsaOaep",
                        "Aes256_Sha256_RsaPss"))
            for (String mode : Arrays.asList("None", "Sign", "SignAndEncrypt"))
                for (String authentication :
                        Arrays.asList(OpcSecurity.USERNAME, OpcSecurity.ANONYMOUS))
                    cases.add(new Object[] {policy, mode, authentication});
        return cases.stream();
    }

    static EndpointDescription endpoint(String policy, String mode, UserTokenPolicy... tokens) {
        return new EndpointDescription(
                "opc.tcp://localhost:4840",
                null,
                null,
                MessageSecurityMode.valueOf(mode),
                SecurityPolicy.valueOf(policy).getUri(),
                tokens,
                OpcSecurity.TCP_PROFILE,
                ubyte(0));
    }

    static UserTokenPolicy token(UserTokenType type, String policyUri) {
        return new UserTokenPolicy(type.name(), type, null, null, policyUri);
    }

    @ParameterizedTest(name = "{0} / {1} / {2}")
    @MethodSource("configurations")
    void validatesEveryPolicyModeAndAuthenticationCombination(
            String policy, String mode, String authentication) {
        TestRunner runner = ProcessorTest.configured(new ProcessorTest.OfflineProcessor());
        runner.setProperty(ConsumeOPCUA.SECURITY_POLICY, policy);
        runner.setProperty(ConsumeOPCUA.SECURITY_MODE, mode);
        runner.setProperty(ConsumeOPCUA.AUTHENTICATION, authentication);
        if ("None".equals(policy)) {
            runner.removeProperty(ConsumeOPCUA.CERTIFICATE);
            runner.removeProperty(ConsumeOPCUA.PRIVATE_KEY);
            runner.removeProperty(ConsumeOPCUA.PKI);
        }
        if (OpcSecurity.ANONYMOUS.equals(authentication)) {
            runner.removeProperty(ConsumeOPCUA.USERNAME);
            runner.removeProperty(ConsumeOPCUA.PASSWORD);
        }
        boolean valid = "None".equals(policy) == "None".equals(mode);
        if (!valid) {
            runner.assertNotValid();
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new OpcSecurity(policy, mode, authentication));
            return;
        }
        runner.assertValid();
        OpcSecurity security = new OpcSecurity(policy, mode, authentication);
        UserTokenType type =
                security.usernameAuthentication()
                        ? UserTokenType.UserName
                        : UserTokenType.Anonymous;
        EndpointDescription selected = endpoint(policy, mode, token(type, null));
        assertSame(selected, security.selectEndpoint(Collections.singletonList(selected)));
        assertEquals(!"None".equals(policy), security.requiresClientIdentity());
        assertEquals(!"None".equals(policy), security.requiresServerTrust(selected));
        UserTokenType wrongType =
                security.usernameAuthentication()
                        ? UserTokenType.Anonymous
                        : UserTokenType.UserName;
        assertFalse(security.matches(endpoint(policy, mode, token(wrongType, null))));
    }

    @Test
    void secureDefaultsRequireCertificatesAndUsername() {
        TestRunner runner = ProcessorTest.configured(new ProcessorTest.OfflineProcessor());
        runner.assertValid();
        for (org.apache.nifi.components.PropertyDescriptor property :
                Arrays.asList(
                        ConsumeOPCUA.CERTIFICATE,
                        ConsumeOPCUA.PRIVATE_KEY,
                        ConsumeOPCUA.PKI,
                        ConsumeOPCUA.USERNAME,
                        ConsumeOPCUA.PASSWORD)) {
            String previous = runner.getProcessContext().getProperty(property).getValue();
            runner.removeProperty(property);
            runner.assertNotValid();
            runner.setProperty(property, previous);
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(ServerCertificateValidation.class)
    void validationModeOnlyChangesTheTrustDirectoryRequirement(ServerCertificateValidation mode) {
        TestRunner runner = ProcessorTest.configured(new ProcessorTest.OfflineProcessor());
        assertEquals(
                "Full",
                runner.getProcessContext()
                        .getProperty(ConsumeOPCUA.SERVER_CERTIFICATE_VALIDATION)
                        .getValue());
        runner.setProperty(ConsumeOPCUA.SERVER_CERTIFICATE_VALIDATION, mode.value());
        runner.removeProperty(ConsumeOPCUA.PKI);
        if (mode == ServerCertificateValidation.NONE) runner.assertValid();
        else runner.assertNotValid();
        runner.setProperty(ConsumeOPCUA.PKI, "/test-pki");
        for (org.apache.nifi.components.PropertyDescriptor property :
                Arrays.asList(
                        ConsumeOPCUA.CERTIFICATE,
                        ConsumeOPCUA.PRIVATE_KEY,
                        ConsumeOPCUA.USERNAME,
                        ConsumeOPCUA.PASSWORD)) {
            String old = runner.getProcessContext().getProperty(property).getValue();
            runner.removeProperty(property);
            runner.assertNotValid();
            runner.setProperty(property, old);
        }
    }

    @Test
    void rejectsUnknownValidationModeAndDoesNotMutateSharedChecks() {
        TestRunner runner = ProcessorTest.configured(new ProcessorTest.OfflineProcessor());
        runner.setProperty(ConsumeOPCUA.SERVER_CERTIFICATE_VALIDATION, "invalid");
        runner.assertNotValid();
        assertThrows(
                IllegalArgumentException.class, () -> ServerCertificateValidation.parse("invalid"));
        assertThrows(IllegalArgumentException.class, () -> ServerCertificateValidation.parse(null));
        Set<org.eclipse.milo.opcua.stack.core.util.validation.ValidationCheck> checks =
                ServerCertificateValidation.CERTIFICATE.optionalChecks();
        assertFalse(
                checks.contains(
                        org.eclipse.milo.opcua.stack.core.util.validation.ValidationCheck
                                .HOSTNAME));
        assertEquals(
                org.eclipse.milo.opcua.stack.core.util.validation.ValidationCheck
                                .ALL_OPTIONAL_CHECKS
                                .size()
                        - 1,
                checks.size());
        checks.clear();
        assertEquals(
                org.eclipse.milo.opcua.stack.core.util.validation.ValidationCheck
                        .ALL_OPTIONAL_CHECKS,
                ServerCertificateValidation.FULL.optionalChecks());
        assertTrue(
                ServerCertificateValidation.FULL
                        .optionalChecks()
                        .contains(
                                org.eclipse.milo.opcua.stack.core.util.validation.ValidationCheck
                                        .HOSTNAME));
    }

    @Test
    void neverDowngradesWhenTheRequestedEndpointIsAbsent() {
        EndpointDescription none = endpoint("None", "None", token(UserTokenType.UserName, null));
        EndpointDescription sign =
                endpoint("Basic256Sha256", "Sign", token(UserTokenType.UserName, null));
        assertThrows(
                IllegalArgumentException.class,
                () -> OpcSecurity.DEFAULT.selectEndpoint(Arrays.asList(none, sign)));
        EndpointDescription secure =
                endpoint("Basic256Sha256", "SignAndEncrypt", token(UserTokenType.UserName, null));
        assertSame(secure, OpcSecurity.DEFAULT.selectEndpoint(Arrays.asList(none, sign, secure)));
    }

    @Test
    void encryptedUsernameTokenNeedsServerTrustWithoutClientIdentity() {
        OpcSecurity security = new OpcSecurity("None", "None", OpcSecurity.USERNAME);
        EndpointDescription encryptedToken =
                endpoint(
                        "None",
                        "None",
                        token(UserTokenType.UserName, SecurityPolicy.Basic256Sha256.getUri()));
        assertTrue(security.matches(encryptedToken));
        assertFalse(security.requiresClientIdentity());
        assertTrue(security.requiresServerTrust(encryptedToken));
        assertFalse(
                security.requiresServerTrust(
                        endpoint("None", "None", token(UserTokenType.UserName, null))));
    }

    @Test
    void ignoresUnsupportedTokensWithoutIgnoringTheRequestedIdentity() {
        OpcSecurity security = new OpcSecurity("None", "None", OpcSecurity.USERNAME);
        UserTokenPolicy supported =
                token(UserTokenType.UserName, SecurityPolicy.Basic256Sha256.getUri());
        EndpointDescription endpoint =
                endpoint(
                        "None",
                        "None",
                        null,
                        token(UserTokenType.Anonymous, null),
                        token(UserTokenType.UserName, "urn:unsupported-policy"),
                        supported);
        assertSame(supported, security.userToken(endpoint));
        assertTrue(security.matches(endpoint));
        assertFalse(security.matches(endpoint("None", "None", (UserTokenPolicy[]) null)));
        assertFalse(security.matches(endpoint("None", "None")));
    }

    static Stream<Object[]> passwordProtectionConfigurations() {
        List<Object[]> cases = new ArrayList<>();
        for (UnencryptedPasswordPolicy allowance : UnencryptedPasswordPolicy.values())
            for (String mode : Arrays.asList("None", "Sign", "SignAndEncrypt"))
                for (String tokenPolicy :
                        Arrays.asList(
                                null,
                                "",
                                SecurityPolicy.None.getUri(),
                                SecurityPolicy.Basic256Sha256.getUri()))
                    cases.add(new Object[] {allowance, mode, tokenPolicy});
        return cases.stream();
    }

    @ParameterizedTest(name = "{0} / {1} / token {2}")
    @MethodSource("passwordProtectionConfigurations")
    void enforcesPasswordProtectionForTheEffectiveTokenPolicy(
            UnencryptedPasswordPolicy allowance, String mode, String tokenPolicy) {
        String policy = "None".equals(mode) ? "None" : "Basic256Sha256";
        OpcSecurity security = new OpcSecurity(policy, mode, OpcSecurity.USERNAME, allowance);
        UserTokenPolicy offered = token(UserTokenType.UserName, tokenPolicy);
        EndpointDescription endpoint = endpoint(policy, mode, offered);
        boolean inherited = tokenPolicy == null || tokenPolicy.isEmpty();
        boolean encrypted =
                inherited
                        ? !"None".equals(policy)
                        : !SecurityPolicy.None.getUri().equals(tokenPolicy);
        boolean accepted =
                encrypted
                        || "SignAndEncrypt".equals(mode)
                        || allowance == UnencryptedPasswordPolicy.ALWAYS
                        || ("None".equals(mode) && allowance != UnencryptedPasswordPolicy.NEVER);
        assertEquals(accepted, security.matches(endpoint));
        if (accepted) {
            assertSame(offered, security.userToken(endpoint));
            assertSame(endpoint, security.selectEndpoint(Collections.singletonList(endpoint)));
            assertEquals(
                    !"None".equals(policy) || encrypted, security.requiresServerTrust(endpoint));
        } else {
            IllegalArgumentException failure =
                    assertThrows(
                            IllegalArgumentException.class, () -> security.userToken(endpoint));
            assertTrue(failure.getMessage().contains("Allow Unencrypted Password"));
            assertTrue(failure.getMessage().contains(mode));
            assertTrue(failure.getMessage().contains("SignAndEncrypt"));
            assertFalse(failure.getMessage().contains("Server Certificate Validation"));
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(UnencryptedPasswordPolicy.class)
    void prefersEncryptedTokensInBothAdvertisedOrdersUnderEverySetting(
            UnencryptedPasswordPolicy allowance) {
        for (String mode : Arrays.asList("None", "Sign", "SignAndEncrypt")) {
            String policy = "None".equals(mode) ? "None" : "Basic256Sha256";
            OpcSecurity security = new OpcSecurity(policy, mode, OpcSecurity.USERNAME, allowance);
            UserTokenPolicy plain =
                    new UserTokenPolicy(
                            "plain",
                            UserTokenType.UserName,
                            null,
                            null,
                            SecurityPolicy.None.getUri());
            UserTokenPolicy encrypted =
                    new UserTokenPolicy(
                            "encrypted",
                            UserTokenType.UserName,
                            null,
                            null,
                            SecurityPolicy.Basic256Sha256.getUri());
            for (UserTokenPolicy[] order :
                    new UserTokenPolicy[][] {{plain, encrypted}, {encrypted, plain}}) {
                UserTokenPolicy[] original = order.clone();
                EndpointDescription endpoint = endpoint(policy, mode, order);
                assertSame(encrypted, security.userToken(endpoint));
                assertTrue(security.requiresServerTrust(endpoint));
                assertArrayEquals(original, order, "Endpoint token order must remain unchanged");
            }
        }
    }

    @Test
    void ranksChannelTokensBeforeOtherRsaTokensAndPreservesTies() {
        OpcSecurity security = new OpcSecurity("Basic256Sha256", "Sign", OpcSecurity.USERNAME);
        UserTokenPolicy other = token(UserTokenType.UserName, SecurityPolicy.Basic256.getUri());
        UserTokenPolicy inherited = token(UserTokenType.UserName, null);
        UserTokenPolicy emptyInherited = token(UserTokenType.UserName, "");
        UserTokenPolicy same =
                token(UserTokenType.UserName, SecurityPolicy.Basic256Sha256.getUri());
        assertSame(
                inherited,
                security.userToken(
                        endpoint(
                                "Basic256Sha256", "Sign", other, inherited, same, emptyInherited)));
        assertSame(
                same,
                security.userToken(
                        endpoint(
                                "Basic256Sha256", "Sign", other, same, emptyInherited, inherited)));
        assertSame(
                other,
                security.userToken(
                        endpoint(
                                "Basic256Sha256",
                                "Sign",
                                other,
                                token(
                                        UserTokenType.UserName,
                                        SecurityPolicy.Basic128Rsa15.getUri()))));
        OpcSecurity none = new OpcSecurity("None", "None", OpcSecurity.USERNAME);
        assertSame(other, none.userToken(endpoint("None", "None", inherited, other, same)));
    }

    @Test
    void skipsUnknownAndUnsupportedPolicyUrisIncludingEccWithoutSdkEnumConstants() {
        OpcSecurity security = new OpcSecurity("None", "None", OpcSecurity.USERNAME);
        UserTokenPolicy supported =
                token(UserTokenType.UserName, SecurityPolicy.Aes256_Sha256_RsaPss.getUri());
        EndpointDescription offered =
                endpoint(
                        "None",
                        "None",
                        null,
                        token(UserTokenType.Anonymous, SecurityPolicy.None.getUri()),
                        token(UserTokenType.UserName, "urn:unknown-policy"),
                        token(
                                UserTokenType.UserName,
                                "http://opcfoundation.org/UA/SecurityPolicy#ECC_nistP256"),
                        token(
                                UserTokenType.UserName,
                                "http://opcfoundation.org/UA/SecurityPolicy#ECC_curve25519"),
                        supported);
        assertSame(supported, security.userToken(offered));
        for (String policy :
                Arrays.asList(
                        "Basic128Rsa15",
                        "Basic256",
                        "Basic256Sha256",
                        "Aes128_Sha256_RsaOaep",
                        "Aes256_Sha256_RsaPss")) {
            UserTokenPolicy rsa =
                    token(UserTokenType.UserName, SecurityPolicy.valueOf(policy).getUri());
            assertSame(rsa, security.userToken(endpoint("None", "None", rsa)));
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(UnencryptedPasswordPolicy.class)
    void anonymousSelectionIgnoresThePasswordProtectionSetting(
            UnencryptedPasswordPolicy allowance) {
        OpcSecurity security = new OpcSecurity("None", "None", OpcSecurity.ANONYMOUS, allowance);
        UserTokenPolicy anonymous = token(UserTokenType.Anonymous, "urn:unused-token-policy");
        EndpointDescription offered =
                endpoint(
                        "None",
                        "None",
                        token(UserTokenType.UserName, SecurityPolicy.Basic256Sha256.getUri()),
                        anonymous);
        assertSame(anonymous, security.userToken(offered));
        assertSame(offered, security.selectEndpoint(Collections.singletonList(offered)));
        assertFalse(security.requiresServerTrust(offered));
    }

    @Test
    void continuesToALaterAcceptedEndpointAndDistinguishesARejectedTokenFromAbsentChannel() {
        OpcSecurity security = new OpcSecurity("Basic256Sha256", "Sign", OpcSecurity.USERNAME);
        EndpointDescription plaintext =
                endpoint(
                        "Basic256Sha256",
                        "Sign",
                        token(UserTokenType.UserName, SecurityPolicy.None.getUri()));
        EndpointDescription unsupported =
                endpoint(
                        "Basic256Sha256", "Sign", token(UserTokenType.UserName, "urn:unsupported"));
        EndpointDescription encrypted =
                endpoint("Basic256Sha256", "Sign", token(UserTokenType.UserName, null));
        assertSame(
                encrypted,
                security.selectEndpoint(Arrays.asList(plaintext, unsupported, encrypted)));
        IllegalArgumentException tokenFailure =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> security.selectEndpoint(Arrays.asList(plaintext, unsupported)));
        assertEquals(
                assertThrows(IllegalArgumentException.class, () -> security.userToken(plaintext))
                        .getMessage(),
                tokenFailure.getMessage());
        IllegalArgumentException channelFailure =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                security.selectEndpoint(
                                        Collections.singletonList(
                                                endpoint(
                                                        "None",
                                                        "None",
                                                        token(UserTokenType.UserName, null)))));
        assertNotEquals(tokenFailure.getMessage(), channelFailure.getMessage());
        assertTrue(channelFailure.getMessage().contains("No OPC TCP endpoint"));
    }

    @Test
    void tokenPreferencesDoNotRelaxExactChannelOrTcpEndpointMatching() {
        UserTokenPolicy encrypted =
                token(UserTokenType.UserName, SecurityPolicy.Basic256Sha256.getUri());
        EndpointDescription exact = endpoint("Basic256Sha256", "Sign", encrypted);
        OpcSecurity security =
                new OpcSecurity(
                        "Basic256Sha256",
                        "Sign",
                        OpcSecurity.USERNAME,
                        UnencryptedPasswordPolicy.ALWAYS);
        assertFalse(security.matches(endpoint("Basic256", "Sign", encrypted)));
        assertFalse(security.matches(endpoint("Basic256Sha256", "SignAndEncrypt", encrypted)));
        for (String url :
                Arrays.asList("https://localhost:4840", "opc.tcp:///missing-host", "::")) {
            EndpointDescription invalid =
                    new EndpointDescription(
                            url,
                            exact.getServer(),
                            exact.getServerCertificate(),
                            exact.getSecurityMode(),
                            exact.getSecurityPolicyUri(),
                            exact.getUserIdentityTokens(),
                            exact.getTransportProfileUri(),
                            exact.getSecurityLevel());
            assertFalse(security.matches(invalid));
        }
        EndpointDescription wrongProfile =
                new EndpointDescription(
                        exact.getEndpointUrl(),
                        exact.getServer(),
                        exact.getServerCertificate(),
                        exact.getSecurityMode(),
                        exact.getSecurityPolicyUri(),
                        exact.getUserIdentityTokens(),
                        "urn:other-profile",
                        exact.getSecurityLevel());
        assertFalse(security.matches(wrongProfile));
        assertFalse(security.matches(null));
    }

    @Test
    void unencryptedConfigurationDoesNotReadUnusedIdentityFiles() throws Exception {
        OpcSettings settings =
                new OpcSettings(
                        "opc.tcp://localhost:4840",
                        null,
                        java.nio.file.Paths.get("/does-not-exist/client.der"),
                        java.nio.file.Paths.get("/does-not-exist/client.key"),
                        "",
                        null,
                        null,
                        null,
                        100,
                        1000,
                        Collections.emptyList(),
                        new OpcSecurity("None", "None", OpcSecurity.ANONYMOUS));
        assertNull(ClientIdentity.load(settings));
        assertEquals("urn:nifi:opcua:subscriber", OpcSecurity.DEFAULT_APPLICATION_URI);
        assertEquals(
                OpcSecurity.DEFAULT_APPLICATION_URI, ClientIdentity.applicationUri(settings, null));
    }
}
