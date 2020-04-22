/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.partitioner;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A token range in a ring. Token ranges have an open lower bound and a closed upper bound.
 *
 * @param <V> The token value type.
 * @param <T> The token type.
 */
public final class TokenRange<V extends Number, T extends Token<V>> {

  private final T start;

  private final T end;

  private final Set<EndPoint> replicas;

  private final TokenFactory<V, T> tokenFactory;

  public TokenRange(
      @NonNull T start,
      @NonNull T end,
      @NonNull Set<EndPoint> replicas,
      @NonNull TokenFactory<V, T> tokenFactory) {
    this.start = start;
    this.end = end;
    this.replicas = replicas;
    this.tokenFactory = tokenFactory;
  }

  /** @return This range's open lower bound. */
  @NonNull
  public T start() {
    return start;
  }

  /** @return This range's closed upper bound. */
  @NonNull
  public T end() {
    return end;
  }

  /** @return The replicas that own this range. */
  @NonNull
  public Set<EndPoint> replicas() {
    return replicas;
  }

  /** @return The {@link TokenFactory} for the tokens in this range. */
  @NonNull
  public TokenFactory<V, T> tokenFactory() {
    return tokenFactory;
  }

  /** @return The range size (i.e. the number of tokens it contains). */
  @NonNull
  public BigInteger size() {
    return tokenFactory.distance(start, end);
  }

  /** @return The ring fraction covered by this range. */
  public double fraction() {
    return tokenFactory.fraction(start, end);
  }

  /**
   * Returns whether this range is empty.
   *
   * <p>A range is empty when start and end are the same token, except if that is the minimum token,
   * in which case the range covers the whole ring (this is consistent with the behavior of CQL
   * range queries).
   *
   * @return whether the range is empty.
   */
  public boolean isEmpty() {
    return start.equals(end) && !start.equals(tokenFactory.minToken());
  }

  /**
   * Returns whether this range wraps around the end of the ring.
   *
   * @return whether this range wraps around.
   */
  public boolean isWrappedAround() {
    return start.compareTo(end) > 0 && !end.equals(tokenFactory.minToken());
  }

  /**
   * Splits this range into a list of two non-wrapping ranges. This will return the range itself if
   * it is non-wrapping, or two ranges otherwise.
   *
   * <p>For example:
   *
   * <ul>
   *   <li>{@code ]1,10]} unwraps to itself;
   *   <li>{@code ]10,1]} unwraps to {@code ]10,min_token]} and {@code ]min_token,1]}.
   * </ul>
   *
   * @return the list of non-wrapping ranges.
   */
  @NonNull
  public List<TokenRange<V, T>> unwrap() {
    if (isWrappedAround()) {
      return ImmutableList.of(
          new TokenRange<>(start, tokenFactory.minToken(), replicas, tokenFactory),
          new TokenRange<>(tokenFactory.minToken(), end, replicas, tokenFactory));
    } else {
      return ImmutableList.of(this);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TokenRange<?, ?> that = (TokenRange<?, ?>) o;
    return Objects.equals(start, that.start)
        && Objects.equals(end, that.end)
        && Objects.equals(replicas, that.replicas)
        && Objects.equals(tokenFactory, that.tokenFactory);
  }

  @Override
  public int hashCode() {
    return Objects.hash(start, end, replicas, tokenFactory);
  }

  @Override
  public String toString() {
    return String.format("(%s,%s]", start, end);
  }
}
