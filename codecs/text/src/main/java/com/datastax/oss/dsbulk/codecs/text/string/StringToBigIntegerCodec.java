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

import static java.util.stream.Collectors.toList;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.dsbulk.codecs.api.util.OverflowStrategy;
import com.datastax.oss.dsbulk.codecs.api.util.TemporalFormat;
import io.netty.util.concurrent.FastThreadLocal;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class StringToBigIntegerCodec extends StringToNumberCodec<BigInteger> {

  public StringToBigIntegerCodec(
      FastThreadLocal<NumberFormat> numberFormat,
      OverflowStrategy overflowStrategy,
      RoundingMode roundingMode,
      TemporalFormat temporalFormat,
      ZoneId timeZone,
      TimeUnit timeUnit,
      ZonedDateTime epoch,
      Map<String, Boolean> booleanStrings,
      List<BigDecimal> booleanNumbers,
      List<String> nullStrings) {
    super(
        TypeCodecs.VARINT,
        numberFormat,
        overflowStrategy,
        roundingMode,
        temporalFormat,
        timeZone,
        timeUnit,
        epoch,
        booleanStrings,
        booleanNumbers.stream().map(BigDecimal::toBigInteger).collect(toList()),
        nullStrings);
  }

  @Override
  public BigInteger externalToInternal(String s) {
    Number number = parseNumber(s);
    if (number == null) {
      return null;
    }
    return narrowNumber(number, BigInteger.class);
  }
}
