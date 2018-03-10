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

public class ConvertingCodecAssert<EXTERNAL, INTERNAL>
    extends AbstractObjectAssert<
        ConvertingCodecAssert<EXTERNAL, INTERNAL>, ConvertingCodec<EXTERNAL, INTERNAL>> {

  public ConvertingCodecAssert(ConvertingCodec<EXTERNAL, INTERNAL> actual) {
    super(actual, ConvertingCodecAssert.class);
  }

  public ConvertsToInternalAssert convertsFromExternal(EXTERNAL external) {
    INTERNAL internal = null;
    try {
      internal = actual.externalToInternal(external);
    } catch (Exception e) {
      fail(
          String.format(
              "Expecting codec to convert from external %s but it threw %s instead",
              external, e.getClass().getName()),
          e);
    }
    return new ConvertsToInternalAssert(actual, external, internal);
  }

  public ConvertsToExternalAssert convertsFromInternal(INTERNAL internal) {
    EXTERNAL external = null;
    try {
      external = actual.internalToExternal(internal);
    } catch (Exception e) {
      fail(
          String.format(
              "Expecting codec to convert from internal %s but it threw %s instead",
              internal, e.getClass().getName()),
          e);
    }
    return new ConvertsToExternalAssert(actual, external, internal);
  }

  public ConvertingCodecAssert<EXTERNAL, INTERNAL> cannotConvertFromExternal(EXTERNAL external) {
    try {
      INTERNAL internal = actual.externalToInternal(external);
      fail(
          String.format(
              "Expecting codec to not convert from external %s but it converted to internal %s",
              external, internal));
    } catch (Exception ignored) {
    }
    return this;
  }

  public ConvertingCodecAssert<EXTERNAL, INTERNAL> cannotConvertFromInternal(INTERNAL internal) {
    try {
      EXTERNAL external = actual.internalToExternal(internal);
      fail(
          String.format(
              "Expecting codec to not convert from internal %s but it converted to external %s",
              internal, external));
    } catch (Exception ignored) {
    }
    return this;
  }

  @SuppressWarnings("ClassCanBeStatic")
  public class ConvertsToInternalAssert extends ConvertingCodecAssert<EXTERNAL, INTERNAL> {

    private final EXTERNAL external;
    private final INTERNAL internal;

    ConvertsToInternalAssert(
        ConvertingCodec<EXTERNAL, INTERNAL> actual, EXTERNAL external, INTERNAL internal) {
      super(actual);
      this.external = external;
      this.internal = internal;
    }

    public ConvertingCodecAssert<EXTERNAL, INTERNAL> toInternal(INTERNAL internal) {
      assertThat(this.internal)
          .overridingErrorMessage(
              "Expecting codec to convert from external %s to internal %s but it converted to %s",
              external, internal, this.internal)
          .isEqualTo(internal);
      return this;
    }
  }

  @SuppressWarnings("ClassCanBeStatic")
  public class ConvertsToExternalAssert extends ConvertingCodecAssert<EXTERNAL, INTERNAL> {

    private final EXTERNAL external;
    private final INTERNAL internal;

    ConvertsToExternalAssert(
        ConvertingCodec<EXTERNAL, INTERNAL> actual, EXTERNAL external, INTERNAL internal) {
      super(actual);
      this.external = external;
      this.internal = internal;
    }

    public ConvertingCodecAssert<EXTERNAL, INTERNAL> toExternal(EXTERNAL external) {
      assertThat(this.external)
          .overridingErrorMessage(
              "Expecting codec to convert from internal %s to external %s but it converted to %s",
              internal, external, this.external)
          .isEqualTo(external);
      return this;
    }
  }
}
