/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
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

  public static final DataType FRUIT_TYPE = DataType.custom("com.datastax.dse.FruitType");

  public static class Fruit {

    public final String name;

    public Fruit(String name) {
      this.name = name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Fruit)) {
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

  public static class FruitCodec extends TypeCodec<Fruit> {

    public FruitCodec() {
      super(FRUIT_TYPE, Fruit.class);
    }

    @Override
    public Fruit parse(String value) throws InvalidTypeException {
      if (value.equalsIgnoreCase("banana")) {
        return new Fruit(value);
      }
      throw new InvalidTypeException("Unknown fruit: " + value);
    }

    @Override
    public String format(Fruit value) throws InvalidTypeException {
      return value.name;
    }

    @Override
    public ByteBuffer serialize(Fruit value, ProtocolVersion protocolVersion)
        throws InvalidTypeException {
      throw new UnsupportedOperationException("irrelevant");
    }

    @Override
    public Fruit deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        throws InvalidTypeException {
      throw new UnsupportedOperationException("irrelevant");
    }
  }
}
