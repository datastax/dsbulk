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

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.DefaultVectorType;
import com.datastax.oss.driver.internal.core.type.codec.VectorCodec;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.dsbulk.codecs.api.ConversionContext;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import java.util.ArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StringToVectorCodecTest {

  private final ArrayList<Float> values = Lists.newArrayList(1.1f, 2.2f, 3.3f, 4.4f, 5.5f);
  private final CqlVector<Float> vector = CqlVector.newInstance(values);
  private final VectorCodec<Float> vectorCodec =
      new VectorCodec<>(new DefaultVectorType(DataTypes.FLOAT, 5), TypeCodecs.FLOAT);

  private StringToVectorCodec<Float> codec;

  @BeforeEach
  void setUp() {
    ConversionContext context = new TextConversionContext().setNullStrings("NULL");
    ConvertingCodecFactory codecFactory = new ConvertingCodecFactory(context);
    codec =
        (StringToVectorCodec<Float>)
            codecFactory.<String, CqlVector<Float>>createConvertingCodec(
                DataTypes.vectorOf(DataTypes.FLOAT, 5), GenericType.STRING, true);
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal(
            vectorCodec.format(vector)) // CQL representation is parsable as a json array
        .toInternal(vector)
        .convertsFromExternal("[1.1,2.2,3.3,4.4,5.5]")
        .toInternal(vector)
        .convertsFromExternal("[1.1000,2.2000,3.3000,4.4000,5.5000]")
        .toInternal(vector)
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal(vector)
        .toExternal(
            "[1.1,2.2,3.3,4.4,5.5]") // this is NOT 100% identical to vector CQL representation
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {

    // Issue 484: now that we're using the dsbulk string-to-subtype converters we should get
    // enforcement of existing dsbulk policies.  For our purposes that means the failure on
    // arithmetic overflow.
    assertThat(codec).cannotConvertFromExternal("6.646329843");

    // dsbulk should effectively treat this as "6.646329843" so we should also fail in this case
    assertThat(codec).cannotConvertFromExternal("[6.646329843]");
  }

  // To keep usage consistent with VectorCodec we confirm that we support encoding when too many
  // elements are available but not when too few are.  Note that it's actually VectorCodec that
  // enforces this constraint so we have to go through encode() rather than the internal/external
  // methods.
  @Test
  void should_fail_to_encode_too_many_or_too_few() {

    ArrayList<Float> tooMany = Lists.newArrayList(values);
    tooMany.add(6.6f);
    CqlVector<Float> tooManyVector = CqlVector.newInstance(tooMany);
    String tooManyString = codec.internalToExternal(tooManyVector);
    ArrayList<Float> tooFew = Lists.newArrayList(values);
    tooFew.remove(0);
    CqlVector<Float> tooFewVector = CqlVector.newInstance(tooFew);
    String tooFewString = codec.internalToExternal(tooFewVector);

    assertThat(codec.encode(tooManyString, ProtocolVersion.DEFAULT)).isNotNull();
    assertThatThrownBy(() -> codec.encode(tooFewString, ProtocolVersion.DEFAULT))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
