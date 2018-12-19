/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Duration;
import com.datastax.driver.core.LocalDate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

public class DataTypesProvider implements ArgumentsProvider {

  private static final List<DataType> dataTypes =
      new ArrayList<>(
          Sets.filter(DataType.allPrimitiveTypes(), type -> type != DataType.counter()));

  @Override
  public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
    List<Arguments> args = new ArrayList<>();
    for (DataType dataType : dataTypes) {
      args.add(Arguments.of(dataType, getFixedValue(dataType)));
    }
    return args.stream();
  }

  private static Object getFixedValue(DataType type) {
    try {
      switch (type.getName()) {
        case CUSTOM:
          break;
        case ASCII:
          return "An ascii string";
        case BIGINT:
          return 42L;
        case BLOB:
          return ByteBuffer.wrap(new byte[] {(byte) 4, (byte) 12, (byte) 1});
        case BOOLEAN:
          return true;
        case COUNTER:
          throw new UnsupportedOperationException("Cannot 'getSomeValue' for counters");
        case DURATION:
          return Duration.from("1h20m3s");
        case DECIMAL:
          return new BigDecimal(
              "3.1415926535897932384626433832795028841971693993751058209749445923078164062862089986280348253421170679");
        case DOUBLE:
          return 3.142519;
        case FLOAT:
          return 3.142519f;
        case INET:
          return InetAddress.getByAddress(new byte[] {(byte) 127, (byte) 0, (byte) 0, (byte) 1});
        case TINYINT:
          return (byte) 25;
        case SMALLINT:
          return (short) 26;
        case INT:
          return 24;
        case TEXT:
          return "A text string";
        case TIMESTAMP:
          return new Date(1352288289L);
        case DATE:
          return LocalDate.fromDaysSinceEpoch(0);
        case TIME:
          return 54012123450000L;
        case UUID:
          return UUID.fromString("087E9967-CCDC-4A9B-9036-05930140A41B");
        case VARCHAR:
          return "A varchar string";
        case VARINT:
          return new BigInteger("123456789012345678901234567890");
        case TIMEUUID:
          return UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66");
        case LIST:
          return Lists.newArrayList(getFixedValue(type.getTypeArguments().get(0)));
        case SET:
          return Sets.newHashSet(getFixedValue(type.getTypeArguments().get(0)));
        case MAP:
          return ImmutableMap.of(
              getFixedValue(type.getTypeArguments().get(0)),
              getFixedValue(type.getTypeArguments().get(1)));
        case UDT:
          break;
        case TUPLE:
          break;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    throw new RuntimeException("Missing handling of " + type);
  }
}
