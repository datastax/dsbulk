/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.string;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.Test;

public class StringToUnknownTypeCodecTest {

  private FruitCodec targetCodec = new FruitCodec();
  private List<String> nullStrings = newArrayList("NULL");
  private Fruit banana = new Fruit("banana");

  @Test
  void should_convert_from_valid_external() {
    StringToUnknownTypeCodec<Fruit> codec =
        new StringToUnknownTypeCodec<>(targetCodec, nullStrings);
    assertThat(codec)
        .convertsFromExternal("banana")
        .toInternal(banana)
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    StringToUnknownTypeCodec<Fruit> codec =
        new StringToUnknownTypeCodec<>(targetCodec, nullStrings);
    assertThat(codec).convertsFromInternal(banana).toExternal("banana");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    StringToUnknownTypeCodec<Fruit> codec =
        new StringToUnknownTypeCodec<>(targetCodec, nullStrings);
    assertThat(codec).cannotConvertFromExternal("not a valid fruit literal");
  }

  private static final DataType FRUIT_TYPE = DataTypes.custom("com.datastax.dse.FruitType");

  public static class Fruit {

    final String name;

    public Fruit(String name) {
      this.name = name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Fruit fruit = (Fruit) o;
      return Objects.equals(name, fruit.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name);
    }
  }

  public static class FruitCodec implements TypeCodec<Fruit> {
    private static GenericType<Fruit> javaType = GenericType.of(Fruit.class);

    @Override
    public Fruit parse(String value) {
      if (value.equalsIgnoreCase("banana")) {
        return new Fruit(value);
      }
      throw new IllegalArgumentException("Unknown fruit: " + value);
    }

    @Override
    public String format(Fruit value) {
      return value.name;
    }

    @Override
    public GenericType<Fruit> getJavaType() {
      return javaType;
    }

    @Override
    public DataType getCqlType() {
      return FRUIT_TYPE;
    }

    @Override
    public ByteBuffer encode(Fruit value, ProtocolVersion protocolVersion) {
      throw new UnsupportedOperationException("irrelevant");
    }

    @Override
    public Fruit decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
      throw new UnsupportedOperationException("irrelevant");
    }
  }
}
