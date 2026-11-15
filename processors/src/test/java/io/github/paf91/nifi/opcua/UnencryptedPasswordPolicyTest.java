/* SPDX-License-Identifier: Apache-2.0 */
package io.github.paf91.nifi.opcua;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.util.TestRunner;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.enumerated.UserTokenType;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.*;
import java.util.stream.Collectors;

class UnencryptedPasswordPolicyTest {
    @Test
    void exposesStoredValuesAndRejectsUnknownValues() {
        assertEquals(
                Arrays.asList("None channels only", "Never", "Always"),
                Arrays.stream(UnencryptedPasswordPolicy.values())
                        .map(UnencryptedPasswordPolicy::value)
                        .collect(Collectors.toList()));
        for (UnencryptedPasswordPolicy policy : UnencryptedPasswordPolicy.values())
            assertSame(policy, UnencryptedPasswordPolicy.parse(policy.value()));
        for (String invalid : Arrays.asList(null, "", "none channels only", "Always (insecure)"))
            assertThrows(
                    IllegalArgumentException.class, () -> UnencryptedPasswordPolicy.parse(invalid));
    }

    @Test
    void propertyDefaultsAndOrderPreserveTheStoredCompatibilityValues() {
        Capturing processor = new Capturing();
        TestRunner runner = ProcessorTest.configured(processor);
        try {
            PropertyDescriptor property = ConsumeOPCUA.ALLOW_UNENCRYPTED_PASSWORD;
            assertEquals("Allow Unencrypted Password", property.getName());
            assertEquals("Allow Unencrypted Password", property.getDisplayName());
            assertTrue(property.isRequired());
            assertFalse(property.isSensitive());
            assertEquals(ExpressionLanguageScope.NONE, property.getExpressionLanguageScope());
            assertEquals(
                    "None channels only",
                    runner.getProcessContext().getProperty(property).getValue());
            assertEquals(
                    Arrays.asList("None channels only", "Never", "Always"),
                    property.getAllowableValues().stream()
                            .map(AllowableValue::getValue)
                            .collect(Collectors.toList()));
            assertEquals(
                    "Always (insecure)", property.getAllowableValues().get(2).getDisplayName());
            List<PropertyDescriptor> descriptors = processor.getPropertyDescriptors();
            assertEquals(
                    descriptors.indexOf(ConsumeOPCUA.AUTHENTICATION) + 1,
                    descriptors.indexOf(property));
            runner.setProperty(property, "Always (insecure)");
            runner.assertNotValid();
            runner.setProperty(property, "invalid");
            runner.assertNotValid();
        } finally {
            runner.shutdown();
        }
    }

    @ParameterizedTest
    @EnumSource(UnencryptedPasswordPolicy.class)
    void verificationUsesTheConfiguredPolicyForUsernameAndAnonymous(
            UnencryptedPasswordPolicy allowance) {
        Capturing processor = new Capturing();
        TestRunner runner = ProcessorTest.configured(processor);
        try {
            runner.setProperty(ConsumeOPCUA.ALLOW_UNENCRYPTED_PASSWORD, allowance.value());
            for (String mode : Arrays.asList("None", "Sign", "SignAndEncrypt")) {
                String channel = "None".equals(mode) ? "None" : "Basic256Sha256";
                runner.setProperty(ConsumeOPCUA.SECURITY_POLICY, channel);
                runner.setProperty(ConsumeOPCUA.SECURITY_MODE, mode);
                runner.assertValid();
                assertEquals(
                        ConfigVerificationResult.Outcome.SUCCESSFUL,
                        processor
                                .verify(
                                        runner.getProcessContext(),
                                        runner.getLogger(),
                                        Collections.emptyMap())
                                .get(0)
                                .getOutcome());
                EndpointDescription plaintext =
                        OpcSecurityTest.endpoint(
                                channel,
                                mode,
                                OpcSecurityTest.token(
                                        UserTokenType.UserName, SecurityPolicy.None.getUri()));
                boolean accepted =
                        "SignAndEncrypt".equals(mode)
                                || allowance == UnencryptedPasswordPolicy.ALWAYS
                                || ("None".equals(mode)
                                        && allowance != UnencryptedPasswordPolicy.NEVER);
                assertEquals(accepted, processor.settings.security().matches(plaintext));
                assertTrue(processor.attempt.isCancelled());
            }
            runner.setProperty(ConsumeOPCUA.AUTHENTICATION, OpcSecurity.ANONYMOUS);
            runner.removeProperty(ConsumeOPCUA.USERNAME);
            runner.removeProperty(ConsumeOPCUA.PASSWORD);
            runner.assertValid();
            assertEquals(
                    ConfigVerificationResult.Outcome.SUCCESSFUL,
                    processor
                            .verify(
                                    runner.getProcessContext(),
                                    runner.getLogger(),
                                    Collections.emptyMap())
                            .get(0)
                            .getOutcome());
            assertTrue(
                    processor
                            .settings
                            .security()
                            .matches(
                                    OpcSecurityTest.endpoint(
                                            "Basic256Sha256",
                                            "SignAndEncrypt",
                                            OpcSecurityTest.token(UserTokenType.Anonymous, null))));
        } finally {
            runner.shutdown();
        }
    }

    @Test
    void threeArgumentConstructorUsesNoneChannelsOnly() {
        OpcSecurity plain = new OpcSecurity("None", "None", OpcSecurity.USERNAME);
        assertTrue(
                plain.matches(
                        OpcSecurityTest.endpoint(
                                "None",
                                "None",
                                OpcSecurityTest.token(UserTokenType.UserName, null))));
        OpcSecurity signed = new OpcSecurity("Basic256Sha256", "Sign", OpcSecurity.USERNAME);
        assertFalse(
                signed.matches(
                        OpcSecurityTest.endpoint(
                                "Basic256Sha256",
                                "Sign",
                                OpcSecurityTest.token(
                                        UserTokenType.UserName, SecurityPolicy.None.getUri()))));
    }

    private static final class Capturing extends ConsumeOPCUA {
        OpcSettings settings;
        ConnectionAttempt attempt;

        @Override
        protected AutoCloseable verifyConnection(
                OpcSettings settings, ConnectionAttempt attempt, ConnectionDiagnostics report) {
            this.settings = settings;
            this.attempt = attempt;
            report.stage("Subscribed");
            return () -> {};
        }
    }
}
