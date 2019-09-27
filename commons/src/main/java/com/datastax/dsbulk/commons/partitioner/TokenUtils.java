/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.partitioner;

import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.internal.core.metadata.token.ByteOrderedToken;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.RandomToken;
import edu.umd.cs.findbugs.annotations.NonNull;

public class TokenUtils {

  @NonNull
  public static Object getTokenValue(@NonNull Token token) {
    Object value;
    if (token instanceof Murmur3Token) {
      value = ((Murmur3Token) token).getValue();
    } else if (token instanceof RandomToken) {
      value = ((RandomToken) token).getValue();
    } else if (token instanceof ByteOrderedToken) {
      value = ((ByteOrderedToken) token).getValue();
    } else {
      throw new IllegalArgumentException("Unknown token type: " + token.getClass());
    }
    return value;
  }
}
