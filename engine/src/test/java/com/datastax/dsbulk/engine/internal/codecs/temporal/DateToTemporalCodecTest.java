/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.temporal;

import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;

import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.driver.extras.codecs.jdk8.LocalDateCodec;
import com.datastax.driver.extras.codecs.jdk8.LocalTimeCodec;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Date;
import org.junit.jupiter.api.Test;

class DateToTemporalCodecTest {

  @Test
  void should_convert_from_java_util_date() {

    assertThat(new DateToTemporalCodec<>(Date.class, InstantCodec.instance, ZoneOffset.UTC))
        .convertsFrom(Date.from(Instant.parse("2010-06-30T00:00:00Z")))
        .to(Instant.parse("2010-06-30T00:00:00Z"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(new DateToTemporalCodec<>(Date.class, InstantCodec.instance, ZoneOffset.ofHours(-1)))
        .convertsFrom(Date.from(Instant.parse("2010-06-30T00:00:00Z")))
        .to(Instant.parse("2010-06-30T00:00:00Z"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(new DateToTemporalCodec<>(Date.class, LocalDateCodec.instance, ZoneOffset.UTC))
        .convertsFrom(Date.from(Instant.parse("2010-06-30T00:00:00Z")))
        .to(LocalDate.parse("2010-06-30"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(Date.class, LocalDateCodec.instance, ZoneOffset.ofHours(-1)))
        .convertsFrom(Date.from(Instant.parse("2010-06-30T00:00:00Z")))
        .to(LocalDate.parse("2010-06-29"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(Date.class, LocalDateCodec.instance, ZoneOffset.ofHours(1)))
        .convertsFrom(Date.from(Instant.parse("2010-06-30T23:59:59Z")))
        .to(LocalDate.parse("2010-07-01"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(new DateToTemporalCodec<>(Date.class, LocalTimeCodec.instance, ZoneOffset.UTC))
        .convertsFrom(Date.from(Instant.parse("1970-01-01T23:59:59Z")))
        .to(LocalTime.parse("23:59:59"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(Date.class, LocalTimeCodec.instance, ZoneOffset.ofHours(1)))
        .convertsFrom(Date.from(Instant.parse("1970-01-01T23:59:59Z")))
        .to(LocalTime.parse("00:59:59"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(Date.class, LocalTimeCodec.instance, ZoneOffset.ofHours(-1)))
        .convertsFrom(Date.from(Instant.parse("1970-01-01T23:59:59Z")))
        .to(LocalTime.parse("22:59:59"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_convert_to_java_util_date() {

    assertThat(new DateToTemporalCodec<>(Date.class, InstantCodec.instance, ZoneOffset.UTC))
        .convertsTo(Instant.parse("2010-06-30T00:00:00Z"))
        .from(Date.from(Instant.parse("2010-06-30T00:00:00Z")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(new DateToTemporalCodec<>(Date.class, InstantCodec.instance, ZoneOffset.ofHours(-1)))
        .convertsTo(Instant.parse("2010-06-30T00:00:00Z"))
        .from(Date.from(Instant.parse("2010-06-30T00:00:00Z")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(new DateToTemporalCodec<>(Date.class, LocalDateCodec.instance, ZoneOffset.UTC))
        .convertsTo(LocalDate.parse("2010-06-30"))
        .from(Date.from(Instant.parse("2010-06-30T00:00:00Z")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(Date.class, LocalDateCodec.instance, ZoneOffset.ofHours(-1)))
        .convertsTo(LocalDate.parse("2010-06-29"))
        .from(Date.from(Instant.parse("2010-06-29T01:00:00Z")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(Date.class, LocalDateCodec.instance, ZoneOffset.ofHours(1)))
        .convertsTo(LocalDate.parse("2010-07-01"))
        .from(Date.from(Instant.parse("2010-06-30T23:00:00Z")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(new DateToTemporalCodec<>(Date.class, LocalTimeCodec.instance, ZoneOffset.UTC))
        .convertsTo(LocalTime.parse("23:59:59"))
        .from(Date.from(Instant.parse("1970-01-01T23:59:59Z")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(Date.class, LocalTimeCodec.instance, ZoneOffset.ofHours(1)))
        .convertsTo(LocalTime.parse("00:59:59"))
        .from(Date.from(Instant.parse("1969-12-31T23:59:59Z")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(Date.class, LocalTimeCodec.instance, ZoneOffset.ofHours(-1)))
        .convertsTo(LocalTime.parse("22:59:59"))
        .from(Date.from(Instant.parse("1970-01-01T23:59:59Z")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_convert_from_java_sql_timestamp() {

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Timestamp.class, InstantCodec.instance, ZoneOffset.UTC))
        .convertsFrom(Timestamp.from(Instant.parse("2010-06-30T00:00:00Z")))
        .to(Instant.parse("2010-06-30T00:00:00Z"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Timestamp.class, InstantCodec.instance, ZoneOffset.ofHours(-1)))
        .convertsFrom(Timestamp.from(Instant.parse("2010-06-30T00:00:00Z")))
        .to(Instant.parse("2010-06-30T00:00:00Z"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Timestamp.class, LocalDateCodec.instance, ZoneOffset.UTC))
        .convertsFrom(Timestamp.from(Instant.parse("2010-06-30T00:00:00Z")))
        .to(LocalDate.parse("2010-06-30"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Timestamp.class, LocalDateCodec.instance, ZoneOffset.ofHours(-1)))
        .convertsFrom(Timestamp.from(Instant.parse("2010-06-30T00:00:00Z")))
        .to(LocalDate.parse("2010-06-29"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Timestamp.class, LocalDateCodec.instance, ZoneOffset.ofHours(1)))
        .convertsFrom(Timestamp.from(Instant.parse("2010-06-30T23:59:59Z")))
        .to(LocalDate.parse("2010-07-01"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Timestamp.class, LocalTimeCodec.instance, ZoneOffset.UTC))
        .convertsFrom(Timestamp.from(Instant.parse("1970-01-01T23:59:59Z")))
        .to(LocalTime.parse("23:59:59"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Timestamp.class, LocalTimeCodec.instance, ZoneOffset.ofHours(1)))
        .convertsFrom(Timestamp.from(Instant.parse("1970-01-01T23:59:59Z")))
        .to(LocalTime.parse("00:59:59"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Timestamp.class, LocalTimeCodec.instance, ZoneOffset.ofHours(-1)))
        .convertsFrom(Timestamp.from(Instant.parse("1970-01-01T23:59:59Z")))
        .to(LocalTime.parse("22:59:59"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_convert_to_java_sql_timestamp() {

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Timestamp.class, InstantCodec.instance, ZoneOffset.UTC))
        .convertsTo(Instant.parse("2010-06-30T00:00:00Z"))
        .from(Timestamp.from(Instant.parse("2010-06-30T00:00:00Z")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Timestamp.class, InstantCodec.instance, ZoneOffset.ofHours(-1)))
        .convertsTo(Instant.parse("2010-06-30T00:00:00Z"))
        .from(Timestamp.from(Instant.parse("2010-06-30T00:00:00Z")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Timestamp.class, LocalDateCodec.instance, ZoneOffset.UTC))
        .convertsTo(LocalDate.parse("2010-06-30"))
        .from(Timestamp.from(Instant.parse("2010-06-30T00:00:00Z")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Timestamp.class, LocalDateCodec.instance, ZoneOffset.ofHours(-1)))
        .convertsTo(LocalDate.parse("2010-06-29"))
        .from(Timestamp.from(Instant.parse("2010-06-29T01:00:00Z")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Timestamp.class, LocalDateCodec.instance, ZoneOffset.ofHours(1)))
        .convertsTo(LocalDate.parse("2010-07-01"))
        .from(Timestamp.from(Instant.parse("2010-06-30T23:00:00Z")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Timestamp.class, LocalTimeCodec.instance, ZoneOffset.UTC))
        .convertsTo(LocalTime.parse("23:59:59"))
        .from(Timestamp.from(Instant.parse("1970-01-01T23:59:59Z")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Timestamp.class, LocalTimeCodec.instance, ZoneOffset.ofHours(1)))
        .convertsTo(LocalTime.parse("00:59:59"))
        .from(Timestamp.from(Instant.parse("1969-12-31T23:59:59Z")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Timestamp.class, LocalTimeCodec.instance, ZoneOffset.ofHours(-1)))
        .convertsTo(LocalTime.parse("22:59:59"))
        .from(Timestamp.from(Instant.parse("1970-01-01T23:59:59Z")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_convert_from_java_sql_date() {

    assertThat(
            new DateToTemporalCodec<>(java.sql.Date.class, InstantCodec.instance, ZoneOffset.UTC))
        .convertsFrom(java.sql.Date.valueOf(LocalDate.parse("2010-06-30")))
        .to(Instant.parse("2010-06-30T00:00:00Z"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Date.class, InstantCodec.instance, ZoneOffset.ofHours(-1)))
        .convertsFrom(java.sql.Date.valueOf(LocalDate.parse("2010-06-30")))
        .to(Instant.parse("2010-06-30T01:00:00Z"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(java.sql.Date.class, LocalDateCodec.instance, ZoneOffset.UTC))
        .convertsFrom(java.sql.Date.valueOf(LocalDate.parse("2010-06-30")))
        .to(LocalDate.parse("2010-06-30"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Date.class, LocalDateCodec.instance, ZoneOffset.ofHours(-1)))
        .convertsFrom(java.sql.Date.valueOf(LocalDate.parse("2010-06-30")))
        .to(LocalDate.parse("2010-06-30"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_convert_to_java_sql_date() {

    assertThat(
            new DateToTemporalCodec<>(java.sql.Date.class, InstantCodec.instance, ZoneOffset.UTC))
        .convertsTo(Instant.parse("2010-06-30T00:00:00Z"))
        .from(java.sql.Date.valueOf(LocalDate.parse("2010-06-30")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Date.class, InstantCodec.instance, ZoneOffset.ofHours(-1)))
        .convertsTo(Instant.parse("2010-06-30T01:00:00Z"))
        .from(java.sql.Date.valueOf(LocalDate.parse("2010-06-30")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(java.sql.Date.class, LocalDateCodec.instance, ZoneOffset.UTC))
        .convertsTo(LocalDate.parse("2010-06-30"))
        .from(java.sql.Date.valueOf(LocalDate.parse("2010-06-30")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Date.class, LocalDateCodec.instance, ZoneOffset.ofHours(-1)))
        .convertsTo(LocalDate.parse("2010-06-30"))
        .from(java.sql.Date.valueOf(LocalDate.parse("2010-06-30")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_convert_from_java_sql_time() {

    assertThat(
            new DateToTemporalCodec<>(java.sql.Time.class, InstantCodec.instance, ZoneOffset.UTC))
        .convertsFrom(java.sql.Time.valueOf(LocalTime.parse("00:00:00")))
        .to(Instant.parse("1970-01-01T00:00:00Z"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Time.class, InstantCodec.instance, ZoneOffset.ofHours(-1)))
        .convertsFrom(java.sql.Time.valueOf(LocalTime.parse("00:00:00")))
        .to(Instant.parse("1970-01-01T01:00:00Z"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(java.sql.Time.class, LocalTimeCodec.instance, ZoneOffset.UTC))
        .convertsFrom(java.sql.Time.valueOf(LocalTime.parse("23:59:59")))
        .to(LocalTime.parse("23:59:59"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Time.class, LocalTimeCodec.instance, ZoneOffset.ofHours(1)))
        .convertsFrom(java.sql.Time.valueOf(LocalTime.parse("23:59:59")))
        .to(LocalTime.parse("23:59:59"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Time.class, LocalTimeCodec.instance, ZoneOffset.ofHours(-1)))
        .convertsFrom(java.sql.Time.valueOf(LocalTime.parse("23:59:59")))
        .to(LocalTime.parse("23:59:59"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_convert_to_java_sql_time() {

    assertThat(
            new DateToTemporalCodec<>(java.sql.Time.class, InstantCodec.instance, ZoneOffset.UTC))
        .convertsTo(Instant.parse("1970-01-01T00:00:00Z"))
        .from(java.sql.Time.valueOf(LocalTime.parse("00:00:00")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Time.class, InstantCodec.instance, ZoneOffset.ofHours(-1)))
        .convertsTo(Instant.parse("1970-01-01T01:00:00Z"))
        .from(java.sql.Time.valueOf(LocalTime.parse("00:00:00")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(java.sql.Time.class, LocalTimeCodec.instance, ZoneOffset.UTC))
        .convertsTo(LocalTime.parse("23:59:59"))
        .from(java.sql.Time.valueOf(LocalTime.parse("23:59:59")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Time.class, LocalTimeCodec.instance, ZoneOffset.ofHours(1)))
        .convertsTo(LocalTime.parse("23:59:59"))
        .from(java.sql.Time.valueOf(LocalTime.parse("23:59:59")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Time.class, LocalTimeCodec.instance, ZoneOffset.ofHours(-1)))
        .convertsTo(LocalTime.parse("23:59:59"))
        .from(java.sql.Time.valueOf(LocalTime.parse("23:59:59")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_not_convert_between_incompatible_types() {

    assertThat(
            new DateToTemporalCodec<>(java.sql.Date.class, LocalTimeCodec.instance, ZoneOffset.UTC))
        .cannotConvertFrom(new java.sql.Date(Instant.parse("1970-01-01T23:59:59Z").toEpochMilli()))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Date.class, LocalTimeCodec.instance, ZoneOffset.ofHours(1)))
        .cannotConvertFrom(new java.sql.Date(Instant.parse("1970-01-01T23:59:59Z").toEpochMilli()))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(java.sql.Time.class, LocalDateCodec.instance, ZoneOffset.UTC))
        .cannotConvertFrom(java.sql.Time.valueOf(LocalTime.parse("23:59:59")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Time.class, LocalDateCodec.instance, ZoneOffset.ofHours(-1)))
        .cannotConvertFrom(java.sql.Time.valueOf(LocalTime.parse("23:59:59")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToTemporalCodec<>(
                java.sql.Time.class, LocalDateCodec.instance, ZoneOffset.ofHours(1)))
        .cannotConvertFrom(java.sql.Time.valueOf(LocalTime.parse("23:59:59")))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);
  }
}
