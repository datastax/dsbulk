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
package com.datastax.oss.dsbulk.codecs.text.string;

import static com.datastax.oss.driver.shaded.guava.common.collect.Lists.newArrayList;
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
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

  public static class FruitCodec implements TypeCodec<Fruit> {

    private static final GenericType<Fruit> JAVA_TYPE = GenericType.of(Fruit.class);

    @Override
    public Fruit parse(String value) {
      if (value != null && value.equalsIgnoreCase("banana")) {
        return new Fruit(value);
      }
      throw new IllegalArgumentException("Unknown fruit: " + value);
    }

    @NonNull
    @Override
    public String format(Fruit value) {
      return value.name;
    }

    @NonNull
    @Override
    public GenericType<Fruit> getJavaType() {
      return JAVA_TYPE;
    }

    @NonNull
    @Override
    public DataType getCqlType() {
      return FRUIT_TYPE;
    }

    @Override
    public ByteBuffer encode(Fruit value, @NonNull ProtocolVersion protocolVersion) {
      throw new UnsupportedOperationException("irrelevant");
    }

    @Override
    public Fruit decode(ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
      throw new UnsupportedOperationException("irrelevant");
    }
  }
}
