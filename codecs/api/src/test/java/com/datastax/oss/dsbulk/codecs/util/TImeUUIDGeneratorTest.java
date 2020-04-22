/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.codecs.util;

import static com.datastax.oss.dsbulk.codecs.util.TimeUUIDGenerator.FIXED;
import static com.datastax.oss.dsbulk.codecs.util.TimeUUIDGenerator.MAX;
import static com.datastax.oss.dsbulk.codecs.util.TimeUUIDGenerator.MIN;
import static com.datastax.oss.dsbulk.codecs.util.TimeUUIDGenerator.RANDOM;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import java.time.Instant;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

class TImeUUIDGeneratorTest {

  @Test
  void should_convert_to_uuid_timestamp() {

    assertThat(TimeUUIDGenerator.toUUIDTimestamp(Instant.EPOCH))
        .isEqualTo(TimeUUIDGenerator.EPOCH_OFFSET);

    assertThat(TimeUUIDGenerator.toUUIDTimestamp(Instant.ofEpochMilli(123456)))
        .isEqualTo(TimeUUIDGenerator.EPOCH_OFFSET + 123456L * 10000L);

    assertThat(TimeUUIDGenerator.toUUIDTimestamp(Instant.ofEpochMilli(-123456)))
        .isEqualTo(TimeUUIDGenerator.EPOCH_OFFSET - 123456L * 10000L);

    assertThat(TimeUUIDGenerator.toUUIDTimestamp(Instant.ofEpochSecond(123, 100)))
        .isEqualTo(TimeUUIDGenerator.EPOCH_OFFSET + 1230000000L + 1L);
  }

  @Test
  void should_convert_from_uuid_timestamp() {

    assertThat(TimeUUIDGenerator.fromUUIDTimestamp(TimeUUIDGenerator.EPOCH_OFFSET))
        .isEqualTo(Instant.EPOCH);

    assertThat(
            TimeUUIDGenerator.fromUUIDTimestamp(TimeUUIDGenerator.EPOCH_OFFSET + 123456L * 10000L))
        .isEqualTo(Instant.ofEpochMilli(123456));

    assertThat(
            TimeUUIDGenerator.fromUUIDTimestamp(TimeUUIDGenerator.EPOCH_OFFSET - 123456L * 10000L))
        .isEqualTo(Instant.ofEpochMilli(-123456));

    assertThat(
            TimeUUIDGenerator.fromUUIDTimestamp(TimeUUIDGenerator.EPOCH_OFFSET + 1230000000L + 1L))
        .isEqualTo(Instant.ofEpochSecond(123, 100));
  }

  @Test
  void should_generate_uuid() {

    // time Uuids with MIN strategy
    assertThat(MIN.generate(ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant()))
        .isEqualTo(
            Uuids.startOf(
                ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli()));

    // time Uuids with MAX strategy
    // the driver's endOf method takes milliseconds and sets all the sub-millisecond digits to their
    // max, that's why we add .000999999
    assertThat(MAX.generate(ZonedDateTime.parse("2017-12-05T12:44:36.000999999+01:00").toInstant()))
        .isEqualTo(
            Uuids.endOf(
                ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli()));

    // time Uuids with FIXED strategy
    assertThat(
            FIXED
                .generate(ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant())
                .timestamp())
        .isEqualTo(
            Uuids.startOf(
                    ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli())
                .timestamp());

    // time Uuids with RANDOM strategy
    assertThat(
            RANDOM
                .generate(ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant())
                .timestamp())
        .isEqualTo(
            Uuids.startOf(
                    ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli())
                .timestamp());
  }
}
