/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.tests;

import static com.datastax.oss.driver.api.core.type.DataTypes.ASCII;
import static com.datastax.oss.driver.api.core.type.DataTypes.BIGINT;
import static com.datastax.oss.driver.api.core.type.DataTypes.BLOB;
import static com.datastax.oss.driver.api.core.type.DataTypes.BOOLEAN;
import static com.datastax.oss.driver.api.core.type.DataTypes.COUNTER;
import static com.datastax.oss.driver.api.core.type.DataTypes.DATE;
import static com.datastax.oss.driver.api.core.type.DataTypes.DECIMAL;
import static com.datastax.oss.driver.api.core.type.DataTypes.DOUBLE;
import static com.datastax.oss.driver.api.core.type.DataTypes.DURATION;
import static com.datastax.oss.driver.api.core.type.DataTypes.FLOAT;
import static com.datastax.oss.driver.api.core.type.DataTypes.INET;
import static com.datastax.oss.driver.api.core.type.DataTypes.INT;
import static com.datastax.oss.driver.api.core.type.DataTypes.SMALLINT;
import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static com.datastax.oss.driver.api.core.type.DataTypes.TIME;
import static com.datastax.oss.driver.api.core.type.DataTypes.TIMESTAMP;
import static com.datastax.oss.driver.api.core.type.DataTypes.TIMEUUID;
import static com.datastax.oss.driver.api.core.type.DataTypes.TINYINT;
import static com.datastax.oss.driver.api.core.type.DataTypes.UUID;
import static com.datastax.oss.driver.api.core.type.DataTypes.VARINT;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

public class DataTypesProvider implements ArgumentsProvider {

  private static final List<DataType> DATA_TYPES =
      Lists.newArrayList(
          ASCII, BIGINT, BLOB, BOOLEAN, DECIMAL, DOUBLE, FLOAT, INT, TIMESTAMP, UUID, VARINT,
          TIMEUUID, INET, DATE, TEXT, TIME, SMALLINT, TINYINT, DURATION);

  @Override
  public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
    List<Arguments> args = new ArrayList<>();
    for (DataType dataType : DATA_TYPES) {
      args.add(Arguments.of(dataType, getFixedValue(dataType)));
    }
    return args.stream();
  }

  private static Object getFixedValue(DataType type) {
    try {
      if (ASCII.equals(type)) {
        return "An ascii string";
      } else if (BIGINT.equals(type)) {
        return 42L;
      } else if (BLOB.equals(type)) {
        return ByteBuffer.wrap(new byte[] {(byte) 4, (byte) 12, (byte) 1});
      } else if (BOOLEAN.equals(type)) {
        return true;
      } else if (COUNTER.equals(type)) {
        throw new UnsupportedOperationException("Cannot 'getSomeValue' for counters");
      } else if (DURATION.equals(type)) {
        return CqlDuration.from("1h20m3s");
      } else if (DECIMAL.equals(type)) {
        return new BigDecimal(
            "3.1415926535897932384626433832795028841971693993751058209749445923078164062862089986280348253421170679");
      } else if (DOUBLE.equals(type)) {
        return 3.142519;
      } else if (FLOAT.equals(type)) {
        return 3.142519f;
      } else if (INET.equals(type)) {
        return InetAddress.getByAddress(new byte[] {(byte) 127, (byte) 0, (byte) 0, (byte) 1});
      } else if (TINYINT.equals(type)) {
        return (byte) 25;
      } else if (SMALLINT.equals(type)) {
        return (short) 26;
      } else if (INT.equals(type)) {
        return 24;
      } else if (TEXT.equals(type)) {
        return "A text string";
      } else if (TIMESTAMP.equals(type)) {
        return Instant.ofEpochMilli(1352288289L);
      } else if (DATE.equals(type)) {
        return LocalDate.ofEpochDay(123456);
      } else if (TIME.equals(type)) {
        return LocalTime.ofNanoOfDay(54012123450000L);
      } else if (UUID.equals(type)) {
        return java.util.UUID.fromString("087E9967-CCDC-4A9B-9036-05930140A41B");
      } else if (VARINT.equals(type)) {
        return new BigInteger("123456789012345678901234567890");
      } else if (TIMEUUID.equals(type)) {
        return java.util.UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66");
      } else if (type instanceof ListType) {
        return Lists.newArrayList(getFixedValue(((ListType) type).getElementType()));
      } else if (type instanceof SetType) {
        return Sets.newHashSet(getFixedValue(((SetType) type).getElementType()));
      } else if (type instanceof MapType) {
        return ImmutableMap.of(
            getFixedValue(((MapType) type).getKeyType()),
            getFixedValue(((MapType) type).getValueType()));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    throw new RuntimeException("Missing handling of " + type);
  }
}
