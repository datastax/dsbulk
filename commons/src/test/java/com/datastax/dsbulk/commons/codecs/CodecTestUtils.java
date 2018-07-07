/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs;

import com.datastax.dsbulk.commons.config.CodecSettings;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.typesafe.config.ConfigFactory;
import java.util.Arrays;

public class CodecTestUtils {
  private static final CodecRegistry CODEC_REGISTRY = new DefaultCodecRegistry("test");

  public static ExtendedCodecRegistry newCodecRegistry(String conf) {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(conf)
                .withFallback(ConfigFactory.load().getConfig("dsbulk.codec")));
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    return settings.createCodecRegistry(new DefaultCodecRegistry("test"));
  }

  public static TupleType newTupleType(DataType... types) {
    return newTupleType(DseProtocolVersion.DSE_V2, CODEC_REGISTRY, types);
  }

  public static TupleType newTupleType(
      ProtocolVersion protocolVersion, CodecRegistry codecRegistry, DataType... types) {
    return new DefaultTupleType(
        Arrays.asList(types),
        new AttachmentPoint() {
          @Override
          public ProtocolVersion protocolVersion() {
            return protocolVersion;
          }

          @Override
          public CodecRegistry codecRegistry() {
            return codecRegistry;
          }
        });
  }
}
