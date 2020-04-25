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
package com.datastax.oss.dsbulk.workflow.commons.settings;

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.dsbulk.codecs.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.json.JsonNodeToUnknownTypeCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringToBigDecimalCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringToBigIntegerCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringToBooleanCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringToByteCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringToDoubleCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringToFloatCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringToInstantCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringToIntegerCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringToListCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringToLocalDateCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringToLocalTimeCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringToLongCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringToMapCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringToSetCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringToShortCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringToTupleCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringToUDTCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringToUUIDCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringToUnknownTypeCodec;
import com.datastax.oss.dsbulk.tests.driver.DriverUtils;
import com.datastax.oss.dsbulk.tests.utils.ReflectionUtils;
import com.datastax.oss.dsbulk.tests.utils.TestConfigUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;
import java.util.List;
import org.junit.jupiter.api.Test;

class CodecSettingsTest {

  @Test
  void should_return_string_converting_codecs() {

    Config config = TestConfigUtils.createTestConfig("dsbulk.codec");
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    ConvertingCodecFactory codecFactory = settings.createCodecFactory(false, false);

    assertThat(codecFactory.createConvertingCodec(DataTypes.BOOLEAN, GenericType.STRING, true))
        .isNotNull()
        .isInstanceOf(StringToBooleanCodec.class);
    assertThat(codecFactory.createConvertingCodec(DataTypes.TINYINT, GenericType.STRING, true))
        .isNotNull()
        .isInstanceOf(StringToByteCodec.class);
    assertThat(codecFactory.createConvertingCodec(DataTypes.SMALLINT, GenericType.STRING, true))
        .isNotNull()
        .isInstanceOf(StringToShortCodec.class);
    assertThat(codecFactory.createConvertingCodec(DataTypes.INT, GenericType.STRING, true))
        .isNotNull()
        .isInstanceOf(StringToIntegerCodec.class);
    assertThat(codecFactory.createConvertingCodec(DataTypes.BIGINT, GenericType.STRING, true))
        .isNotNull()
        .isInstanceOf(StringToLongCodec.class);
    assertThat(codecFactory.createConvertingCodec(DataTypes.FLOAT, GenericType.STRING, true))
        .isNotNull()
        .isInstanceOf(StringToFloatCodec.class);
    assertThat(codecFactory.createConvertingCodec(DataTypes.DOUBLE, GenericType.STRING, true))
        .isNotNull()
        .isInstanceOf(StringToDoubleCodec.class);
    assertThat(codecFactory.createConvertingCodec(DataTypes.VARINT, GenericType.STRING, true))
        .isNotNull()
        .isInstanceOf(StringToBigIntegerCodec.class);
    assertThat(codecFactory.createConvertingCodec(DataTypes.DECIMAL, GenericType.STRING, true))
        .isNotNull()
        .isInstanceOf(StringToBigDecimalCodec.class);
    assertThat(codecFactory.createConvertingCodec(DataTypes.DATE, GenericType.STRING, true))
        .isNotNull()
        .isInstanceOf(StringToLocalDateCodec.class);
    assertThat(codecFactory.createConvertingCodec(DataTypes.TIME, GenericType.STRING, true))
        .isNotNull()
        .isInstanceOf(StringToLocalTimeCodec.class);
    assertThat(codecFactory.createConvertingCodec(DataTypes.TIMESTAMP, GenericType.STRING, true))
        .isNotNull()
        .isInstanceOf(StringToInstantCodec.class);
    assertThat(codecFactory.createConvertingCodec(DataTypes.UUID, GenericType.STRING, true))
        .isNotNull()
        .isInstanceOf(StringToUUIDCodec.class);
    assertThat(codecFactory.createConvertingCodec(DataTypes.TIMEUUID, GenericType.STRING, true))
        .isNotNull()
        .isInstanceOf(StringToUUIDCodec.class);
  }

  @Test
  void should_return_codecs_for_tokenizable_fields() {

    Config config = TestConfigUtils.createTestConfig("dsbulk.codec");
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    ConvertingCodecFactory codecFactory = settings.createCodecFactory(false, false);

    assertThat(
            codecFactory.createConvertingCodec(
                DataTypes.listOf(DataTypes.INT), GenericType.STRING, true))
        .isNotNull()
        .isInstanceOf(StringToListCodec.class);
    assertThat(
            codecFactory.createConvertingCodec(
                DataTypes.setOf(DataTypes.DOUBLE), GenericType.STRING, true))
        .isNotNull()
        .isInstanceOf(StringToSetCodec.class);
    assertThat(
            codecFactory.createConvertingCodec(
                DataTypes.mapOf(DataTypes.TIME, DataTypes.TEXT), GenericType.STRING, true))
        .isNotNull()
        .isInstanceOf(StringToMapCodec.class);
    TupleType tupleType = DriverUtils.mockTupleType(DataTypes.INT, DataTypes.DOUBLE);
    assertThat(codecFactory.createConvertingCodec(tupleType, GenericType.STRING, true))
        .isNotNull()
        .isInstanceOf(StringToTupleCodec.class);
    UserDefinedType udtType =
        new UserDefinedTypeBuilder("ks", "udt")
            .withField("f1", DataTypes.INT)
            .withField("f2", DataTypes.DOUBLE)
            .build();
    assertThat(codecFactory.createConvertingCodec(udtType, GenericType.STRING, true))
        .isNotNull()
        .isInstanceOf(StringToUDTCodec.class);
  }

  @Test
  void should_return_rounding_codecs() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.codec", "roundingStrategy", "UP", "formatNumbers", "true");
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    ConvertingCodecFactory codecFactory = settings.createCodecFactory(false, false);
    ConvertingCodec<String, Float> codec =
        codecFactory.createConvertingCodec(DataTypes.FLOAT, GenericType.STRING, true);
    assertThat(codec.internalToExternal(0.123f)).isEqualTo("0.13");
  }

  @Test
  void should_return_codecs_honoring_overflow_strategy() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.codec", "overflowStrategy", "TRUNCATE");
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    ConvertingCodecFactory codecFactory = settings.createCodecFactory(false, false);
    ConvertingCodec<String, Byte> codec =
        codecFactory.createConvertingCodec(DataTypes.TINYINT, GenericType.STRING, true);
    assertThat(codec.externalToInternal("128")).isEqualTo((byte) 127);
  }

  @SuppressWarnings("unchecked")
  @Test
  void should_create_settings_when_null_words_are_specified() {
    {
      Config config = TestConfigUtils.createTestConfig("dsbulk.codec", "nullStrings", "[NULL]");
      CodecSettings codecSettings = new CodecSettings(config);
      codecSettings.init();

      assertThat((List<String>) ReflectionUtils.getInternalState(codecSettings, "nullStrings"))
          .containsOnly("NULL");
    }
    {
      Config config =
          TestConfigUtils.createTestConfig("dsbulk.codec", "nullStrings", "[NIL, NULL]");
      CodecSettings codecSettings = new CodecSettings(config);
      codecSettings.init();

      assertThat((List<String>) ReflectionUtils.getInternalState(codecSettings, "nullStrings"))
          .containsOnly("NIL", "NULL");
    }
    {
      Config config = TestConfigUtils.createTestConfig("dsbulk.codec", "nullStrings", "[\"NULL\"]");
      CodecSettings codecSettings = new CodecSettings(config);
      codecSettings.init();

      assertThat((List<String>) ReflectionUtils.getInternalState(codecSettings, "nullStrings"))
          .containsOnly("NULL");
    }
    {
      Config config =
          TestConfigUtils.createTestConfig("dsbulk.codec", "nullStrings", "[\"NIL\", \"NULL\"]");
      CodecSettings codecSettings = new CodecSettings(config);
      codecSettings.init();

      assertThat((List<String>) ReflectionUtils.getInternalState(codecSettings, "nullStrings"))
          .containsOnly("NIL", "NULL");
    }
  }

  @Test
  void should_throw_exception_when_nullStrings_not_a_list() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.codec", "nullStrings", null);
    CodecSettings settings = new CodecSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.codec.nullStrings, expecting LIST, got NULL");
  }

  @Test
  void should_return_string_to_unknown_type_codec_for_dynamic_composite_type() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.codec");
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    ConvertingCodecFactory codecFactory = settings.createCodecFactory(false, false);
    assertThat(
            codecFactory.createConvertingCodec(
                DataTypes.custom("org.apache.cassandra.db.marshal.DynamicCompositeType"),
                GenericType.STRING,
                true))
        .isNotNull()
        .isInstanceOf(StringToUnknownTypeCodec.class);
    assertThat(
            codecFactory.createConvertingCodec(
                DataTypes.custom("org.apache.cassandra.db.marshal.DynamicCompositeType"),
                GenericType.of(JsonNode.class),
                true))
        .isNotNull()
        .isInstanceOf(JsonNodeToUnknownTypeCodec.class);
  }
}
