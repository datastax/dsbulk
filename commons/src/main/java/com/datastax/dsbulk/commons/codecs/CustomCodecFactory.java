/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface CustomCodecFactory {
  /**
   * Hook for allowing applications to inject their own converting codecs.
   *
   * @return a codec if one can be created based on the args, null otherwise.
   */
  @Nullable ConvertingCodec<?, ?> codecFor(
      @NotNull ExtendedCodecRegistry codecRegistry,
      @NotNull DataType cqlType,
      @NotNull GenericType<?> javaType);
}
