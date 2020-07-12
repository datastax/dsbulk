/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.workflow.commons.auth;

import static com.datastax.oss.dsbulk.io.IOUtils.assertAccessibleFile;

import com.datastax.dse.driver.api.core.auth.DseGssApiAuthProviderBase.GssApiOptions;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.internal.core.auth.ProgrammaticPlainTextAuthProvider;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.config.ConfigUtils;
import com.typesafe.config.Config;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthProviderFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuthProviderFactory.class);

  @Nullable
  public static AuthProvider createAuthProvider(Config config) {

    String authProvider = config.getString("provider");

    // If the user specified a username or a password, but no auth provider, infer
    // PlainTextAuthProvider
    if (authProvider.equals("None") && config.hasPath("username") && config.hasPath("password")) {
      LOGGER.info(
          "Username and password provided but auth provider not specified, inferring PlainTextAuthProvider");
      authProvider = "PlainTextAuthProvider";
    }

    if (authProvider.equals("None")) {
      return null;
    }

    String authorizationId = "";
    if (config.hasPath("authorizationId")) {
      authorizationId = config.getString("authorizationId");
    }

    switch (authProvider.toLowerCase()) {
      case "plaintextauthprovider":
        return createPlainTextAuthProvider(config, authProvider, authorizationId);
      case "dseplaintextauthprovider":
        LOGGER.warn(
            "The DsePlainTextAuthProvider is deprecated. Please use PlainTextAuthProvider instead.");
        return createPlainTextAuthProvider(config, authProvider, authorizationId);

      case "dsegssapiauthprovider":
        return createGssApiAuthProvider(config, authProvider, authorizationId);

      default:
        throw new IllegalArgumentException(
            String.format(
                "Invalid value for dsbulk.driver.auth.provider, expecting one of "
                    + "PlainTextAuthProvider, DsePlainTextAuthProvider, or DseGSSAPIAuthProvider, got: '%s'",
                authProvider));
    }
  }

  private static AuthProvider createPlainTextAuthProvider(
      Config config, String authProvider, String authorizationId) {
    checkHasCredentials(config, authProvider);
    return new ProgrammaticPlainTextAuthProvider(
        config.getString("username"), config.getString("password"), authorizationId);
  }

  private static AuthProvider createGssApiAuthProvider(
      Config config, String authProvider, String authorizationId) {
    if (!config.hasPath("saslService")) {
      throw new IllegalArgumentException(
          String.format(
              "dsbulk.driver.auth.saslService must be provided with %s. "
                  + "dsbulk.driver.auth.principal, dsbulk.driver.auth.keyTab, and "
                  + "dsbulk.driver.auth.authorizationId are optional.",
              authProvider));
    }
    String authSaslService = config.getString("saslService");

    String authPrincipal = null;
    if (config.hasPath("principal")) {
      authPrincipal = config.getString("principal");
    }

    Path authKeyTab = null;
    if (config.hasPath("keyTab")) {
      authKeyTab = ConfigUtils.getPath(config, "keyTab");
      assertAccessibleFile(authKeyTab, "Keytab file");

      // When using a keytab, we must have a principal. If the user didn't provide one,
      // try to get the first principal from the keytab.
      if (authPrincipal == null) {
        // Best effort: get the first principal in the keytab, if possible.
        // We use reflection because we're referring to sun internal kerberos classes:
        // sun.security.krb5.internal.ktab.KeyTab;
        // sun.security.krb5.internal.ktab.KeyTabEntry;
        // The code below is equivalent to the following:
        //
        // keyTab = KeyTab.getInstance(authKeyTab.toString());
        // KeyTabEntry[] entries = keyTab.getEntries();
        // if (entries.length > 0) {
        //   authPrincipal = entries[0].getService().getName();
        //   LOGGER.debug("Found Kerberos principal %s in %s", authPrincipal, authKeyTab);
        // } else {
        //   throw new IllegalArgumentException(
        //   String.format("Could not find any principals in %s", authKeyTab));
        // }

        try {
          Class<?> keyTabClazz = Class.forName("sun.security.krb5.internal.ktab.KeyTab");
          Class<?> keyTabEntryClazz = Class.forName("sun.security.krb5.internal.ktab.KeyTabEntry");
          Class<?> principalNameClazz = Class.forName("sun.security.krb5.PrincipalName");

          Method getInstanceMethod = keyTabClazz.getMethod("getInstance", String.class);
          Method getEntriesMethod = keyTabClazz.getMethod("getEntries");
          Method getServiceMethod = keyTabEntryClazz.getMethod("getService");
          Method getNameMethod = principalNameClazz.getMethod("getName");

          Object keyTab = getInstanceMethod.invoke(null, authKeyTab.toString());
          Object[] entries = (Object[]) getEntriesMethod.invoke(keyTab);

          if (entries.length > 0) {
            authPrincipal = (String) getNameMethod.invoke(getServiceMethod.invoke(entries[0]));
            LOGGER.debug("Found Kerberos principal {} in {}", authPrincipal, authKeyTab);
          } else {
            throw new IllegalArgumentException(
                String.format("Could not find any principals in %s", authKeyTab));
          }
        } catch (Exception e) {
          throw new IllegalArgumentException(
              String.format("Could not find any principals in %s", authKeyTab), e);
        }
      }
    }

    Configuration configuration;
    if (authKeyTab != null) {
      configuration = new KeyTabConfiguration(authPrincipal, authKeyTab.toString());
    } else {
      configuration = new TicketCacheConfiguration(authPrincipal);
    }

    GssApiOptions options =
        GssApiOptions.builder()
            .withLoginConfiguration(configuration)
            .withAuthorizationId(authorizationId)
            .withSaslProtocol(authSaslService)
            .build();
    return new BulkGssApiAuthProvider(options);
  }

  private static void checkHasCredentials(Config config, String authProvider) {
    if (!config.hasPath("username") || !config.hasPath("password")) {
      throw new IllegalArgumentException(
          "Both dsbulk.driver.auth.username and dsbulk.driver.auth.password must be provided with "
              + authProvider);
    }
  }

  @VisibleForTesting
  public static class KeyTabConfiguration extends Configuration {

    private final String principal;
    private final String keyTab;

    KeyTabConfiguration(String principal, String keyTab) {
      this.principal = principal;
      this.keyTab = keyTab;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      Map<String, String> options =
          ImmutableMap.<String, String>builder()
              .put("principal", principal)
              .put("useKeyTab", "true")
              .put("refreshKrb5Config", "true")
              .put("keyTab", keyTab)
              .build();

      return new AppConfigurationEntry[] {
        new AppConfigurationEntry(
            "com.sun.security.auth.module.Krb5LoginModule",
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            options)
      };
    }
  }

  @VisibleForTesting
  public static class TicketCacheConfiguration extends Configuration {

    private final String principal;

    TicketCacheConfiguration(String principal) {
      this.principal = principal;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      ImmutableMap.Builder<String, String> builder =
          ImmutableMap.<String, String>builder()
              .put("useTicketCache", "true")
              .put("refreshKrb5Config", "true")
              .put("renewTGT", "true");

      if (principal != null) {
        builder.put("principal", principal);
      }

      Map<String, String> options = builder.build();

      return new AppConfigurationEntry[] {
        new AppConfigurationEntry(
            "com.sun.security.auth.module.Krb5LoginModule",
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            options)
      };
    }
  }
}
