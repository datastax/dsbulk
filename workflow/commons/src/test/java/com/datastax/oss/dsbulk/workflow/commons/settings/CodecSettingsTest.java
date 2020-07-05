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
import static org.assertj.core.api.Assertions.entry;

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
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
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

  @Test
  void should_create_settings_when_null_words_are_specified() {
    {
      Config config = TestConfigUtils.createTestConfig("dsbulk.codec", "nullStrings", "[NULL]");
      CodecSettings codecSettings = new CodecSettings(config);
      codecSettings.init();
      @SuppressWarnings("unchecked")
      List<String> nullStrings =
          (List<String>) ReflectionUtils.getInternalState(codecSettings, "nullStrings");
      assertThat(nullStrings).containsOnly("NULL");
    }
    {
      Config config =
          TestConfigUtils.createTestConfig("dsbulk.codec", "nullStrings", "[NIL, NULL]");
      CodecSettings codecSettings = new CodecSettings(config);
      codecSettings.init();
      @SuppressWarnings("unchecked")
      List<String> nullStrings =
          (List<String>) ReflectionUtils.getInternalState(codecSettings, "nullStrings");
      assertThat(nullStrings).containsOnly("NIL", "NULL");
    }
    {
      Config config = TestConfigUtils.createTestConfig("dsbulk.codec", "nullStrings", "[\"NULL\"]");
      CodecSettings codecSettings = new CodecSettings(config);
      codecSettings.init();
      @SuppressWarnings("unchecked")
      List<String> nullStrings =
          (List<String>) ReflectionUtils.getInternalState(codecSettings, "nullStrings");
      assertThat(nullStrings).containsOnly("NULL");
    }
    {
      Config config =
          TestConfigUtils.createTestConfig("dsbulk.codec", "nullStrings", "[\"NIL\", \"NULL\"]");
      CodecSettings codecSettings = new CodecSettings(config);
      codecSettings.init();
      @SuppressWarnings("unchecked")
      List<String> nullStrings =
          (List<String>) ReflectionUtils.getInternalState(codecSettings, "nullStrings");
      assertThat(nullStrings).containsOnly("NIL", "NULL");
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
  void should_throw_exception_when_epoch_not_in_ISO_ZONED_DATE_TIME() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.codec", "epoch", "NotAValidTemporal");
    CodecSettings settings = new CodecSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.codec.epoch, expecting temporal in ISO_ZONED_DATE_TIME format, got 'NotAValidTemporal'");
  }

  @Test
  void should_throw_exception_when_booleanNumbers_not_a_list_of_two_elements() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.codec", "booleanNumbers", "[0,1,2]");
    CodecSettings settings = new CodecSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.codec.booleanNumbers, expecting list with two numbers, got '[0, 1, 2]'");
  }

  @Test
  void should_throw_exception_when_booleanNumbers_not_a_list_of_numbers() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.codec", "booleanNumbers", "[0,NotANumber]");
    CodecSettings settings = new CodecSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.codec.booleanNumbers, expecting list with two numbers, got '[0, NotANumber]'");
  }

  @Test
  void should_create_settings_when_valid_booleanNumbers() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.codec", "booleanNumbers", "[-1,0.5]");
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    @SuppressWarnings("unchecked")
    List<BigDecimal> booleanNumbers =
        (List<BigDecimal>) ReflectionUtils.getInternalState(settings, "booleanNumbers");
    assertThat(booleanNumbers).containsExactly(new BigDecimal("-1"), new BigDecimal("0.5"));
  }

  @Test
  void should_throw_exception_when_booleanStrings_empty() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.codec", "booleanStrings", "[]");
    CodecSettings settings = new CodecSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.codec.booleanStrings, expecting list with at least one true:false pair, got '[]'");
  }

  @Test
  void should_throw_exception_when_booleanStrings_not_a_list_of_tokens() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.codec", "booleanStrings", "[NotATrueFalsePair]");
    CodecSettings settings = new CodecSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.codec.booleanStrings, expecting list with at least one true:false pair, got '[NotATrueFalsePair]'");
  }

  @Test
  void should_create_settings_when_valid_booleanStrings() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.codec", "booleanStrings", "[\"VRAI:FAUX\",\"Σωστό:Λάθος\"]");
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    @SuppressWarnings("unchecked")
    Map<String, Boolean> booleanInputWords =
        (Map<String, Boolean>) ReflectionUtils.getInternalState(settings, "booleanInputWords");
    assertThat(booleanInputWords)
        .containsOnly(
            entry("vrai", true), entry("faux", false), entry("σωστό", true), entry("λάθος", false));
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
