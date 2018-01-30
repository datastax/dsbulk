/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import org.assertj.core.api.AbstractObjectAssert;

public class ConvertingCodecAssert<FROM, TO>
    extends AbstractObjectAssert<ConvertingCodecAssert<FROM, TO>, ConvertingCodec<FROM, TO>> {

  public ConvertingCodecAssert(ConvertingCodec<FROM, TO> actual) {
    super(actual, ConvertingCodecAssert.class);
  }

  public ConvertsFromAssert convertsFrom(FROM from) {
    TO to = null;
    try {
      to = actual.convertFrom(from);
    } catch (Exception e) {
      fail(
          String.format(
              "Expecting codec to convert from %s but it threw %s instead",
              from, e.getClass().getName()),
          e);
    }
    return new ConvertsFromAssert(actual, from, to);
  }

  public ConvertsToAssert convertsTo(TO to) {
    FROM from = null;
    try {
      from = actual.convertTo(to);
    } catch (Exception e) {
      fail(
          String.format(
              "Expecting codec to convert to %s but it threw %s instead",
              to, e.getClass().getName()),
          e);
    }
    return new ConvertsToAssert(actual, from, to);
  }

  public ConvertingCodecAssert<FROM, TO> cannotConvertFrom(FROM from) {
    try {
      TO to = actual.convertFrom(from);
      fail(
          String.format("Expecting codec to not convert from %s but it converted to %s", from, to));
    } catch (Exception ignored) {
    }
    return this;
  }

  public ConvertingCodecAssert<FROM, TO> cannotConvertTo(TO to) {
    try {
      FROM from = actual.convertTo(to);
      fail(
          String.format("Expecting codec to not convert to %s but it converted from %s", to, from));
    } catch (Exception ignored) {
    }
    return this;
  }

  @SuppressWarnings("ClassCanBeStatic")
  public class ConvertsFromAssert extends ConvertingCodecAssert<FROM, TO> {

    private final FROM from;
    private final TO to;

    ConvertsFromAssert(ConvertingCodec<FROM, TO> actual, FROM from, TO to) {
      super(actual);
      this.from = from;
      this.to = to;
    }

    public ConvertingCodecAssert<FROM, TO> to(TO to) {
      assertThat(this.to)
          .overridingErrorMessage(
              "Expecting codec to convert to %s from %s but it was to %s", to, from, this.to)
          .isEqualTo(to);
      return this;
    }
  }

  @SuppressWarnings("ClassCanBeStatic")
  public class ConvertsToAssert extends ConvertingCodecAssert<FROM, TO> {

    private final FROM from;
    private final TO to;

    ConvertsToAssert(ConvertingCodec<FROM, TO> actual, FROM from, TO to) {
      super(actual);
      this.from = from;
      this.to = to;
    }

    public ConvertingCodecAssert<FROM, TO> from(FROM from) {
      assertThat(this.from)
          .overridingErrorMessage(
              "Expecting codec to convert back from %s to %s but it was to %s", to, from, this.from)
          .isEqualTo(from);
      return this;
    }
  }
}
