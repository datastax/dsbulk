/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.collection;

import static com.datastax.dsbulk.commons.codecs.CodecTestUtils.newCodecRegistry;
import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ListToUDTCodecTest {
  private final UserDefinedType udt1 =
      new UserDefinedTypeBuilder("ks", "udt")
          .withField("f1a", DataTypes.INT)
          .withField("f1b", DataTypes.DOUBLE)
          .build();

  private final UdtValue udt1Empty = udt1.newValue().setToNull("f1a").setToNull("f1b");

  private final UdtValue udt1Value = udt1.newValue().setInt("f1a", 42).setDouble("f1b", 0.12d);

  private ListToUDTCodec<String> udtCodec1 =
      (ListToUDTCodec<String>)
          newCodecRegistry("").codecFor(udt1, GenericType.listOf(GenericType.STRING));
  private List<String> stringList = Arrays.asList("42", "0.12");

  @Test
  void should_convert_from_valid_external() {
    assertThat(udtCodec1)
        .convertsFromExternal(stringList)
        .toInternal(udt1Value)
        .convertsFromExternal(Arrays.asList("", null))
        .toInternal(udt1Empty)
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @SuppressWarnings("unchecked")
  @Test
  void should_convert_from_valid_internal() {
    assertThat(udtCodec1)
        .convertsFromInternal(udt1Value)
        .toExternal(stringList)
        .convertsFromInternal(udt1.newValue())
        .toExternal(Arrays.asList("", ""))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(udtCodec1).cannotConvertFromExternal(Collections.singletonList(""));
  }
}
