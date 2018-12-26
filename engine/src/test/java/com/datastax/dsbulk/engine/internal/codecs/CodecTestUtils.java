/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.typesafe.config.ConfigFactory;

public class CodecTestUtils {

  public static ExtendedCodecRegistry newCodecRegistry(String conf) {
    return newCodecRegistry(conf, false, false);
  }

  public static ExtendedCodecRegistry newCodecRegistry(
      String conf, boolean allowExtraFields, boolean allowMissingFields) {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(conf)
                .withFallback(ConfigFactory.load().getConfig("dsbulk.codec")));
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    return settings.createCodecRegistry(new CodecRegistry(), allowExtraFields, allowMissingFields);
  }
}
