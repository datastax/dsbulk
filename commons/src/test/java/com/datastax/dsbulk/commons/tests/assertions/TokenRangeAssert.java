/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.assertions;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;

import com.datastax.dsbulk.commons.partitioner.Token;
import com.datastax.dsbulk.commons.partitioner.TokenFactory;
import com.datastax.dsbulk.commons.partitioner.TokenRange;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.data.Offset;

@SuppressWarnings("UnusedReturnValue")
public class TokenRangeAssert<V extends Number, T extends Token<V>>
    extends AbstractObjectAssert<TokenRangeAssert<V, T>, TokenRange<V, T>> {

  TokenRangeAssert(TokenRange<V, T> actual) {
    super(actual, TokenRangeAssert.class);
  }

  public TokenRangeAssert<V, T> startsWith(Object value) {
    assertThat(actual.start().value())
        .overridingErrorMessage(
            "Expecting %s to start with %s but it starts with %s", actual, value, actual.start())
        .isEqualTo(value);
    return this;
  }

  public TokenRangeAssert<V, T> endsWith(Object value) {
    assertThat(actual.end().value())
        .overridingErrorMessage(
            "Expecting %s to end with %s but it ends with %s", actual, value, actual.end())
        .isEqualTo(value);
    return this;
  }

  public TokenRangeAssert<V, T> startsWith(Token<V> token) {
    assertThat(actual.start())
        .overridingErrorMessage(
            "Expecting %s to start with %s but it starts with %s", actual, token, actual.start())
        .isEqualTo(token);
    return this;
  }

  public TokenRangeAssert<V, T> endsWith(Token<V> token) {
    assertThat(actual.end())
        .overridingErrorMessage(
            "Expecting %s to end with %s but it ends with %s", actual, token, actual.end())
        .isEqualTo(token);
    return this;
  }

  public TokenRangeAssert<V, T> hasRange(Object start, Object end) {
    assertThat(actual.start().value())
        .overridingErrorMessage(
            "Expecting %s to start with %s but it starts with %s", actual, start, actual.start())
        .isEqualTo(start);
    assertThat(actual.end().value())
        .overridingErrorMessage(
            "Expecting %s to end with %s but it ends with %s", actual, end, actual.start())
        .isEqualTo(end);
    return this;
  }

  public TokenRangeAssert<V, T> hasSize(long size) {
    assertThat(actual.size())
        .overridingErrorMessage(
            "Expecting %s to have size %d but it has size %d", actual, size, actual.size())
        .isEqualTo(size);
    return this;
  }

  public TokenRangeAssert<V, T> hasSize(BigInteger size) {
    assertThat(actual.size())
        .overridingErrorMessage(
            "Expecting %s to have size %d but it has size %d", actual, size, actual.size())
        .isEqualTo(size);
    return this;
  }

  public TokenRangeAssert<V, T> hasFraction(double fraction) {
    assertThat(actual.fraction())
        .overridingErrorMessage(
            "Expecting %s to have fraction %f but it has fraction %f",
            actual, fraction, actual.fraction())
        .isEqualTo(fraction);
    return this;
  }

  public TokenRangeAssert<V, T> hasFraction(double fraction, Offset<Double> offset) {
    assertThat(actual.fraction())
        .overridingErrorMessage(
            "Expecting %s to have fraction %f (+- %s) but it has fraction %f",
            actual, fraction, offset, actual.fraction())
        .isEqualTo(fraction, offset);
    return this;
  }

  public TokenRangeAssert<V, T> hasReplicas(Node... hosts) {
    Set<EndPoint> expected =
        Arrays.stream(hosts).map(Node::getEndPoint).collect(Collectors.toSet());
    assertThat(actual.replicas())
        .overridingErrorMessage(
            "Expecting %s to have replicas %s but it had %s", actual, expected, actual.replicas())
        .isEqualTo(expected);
    return this;
  }

  public TokenRangeAssert<V, T> isEmpty() {
    assertThat(actual.isEmpty())
        .overridingErrorMessage("Expecting %s to be empty but it was not", actual)
        .isTrue();
    return this;
  }

  public TokenRangeAssert<V, T> isNotEmpty() {
    assertThat(actual.isEmpty())
        .overridingErrorMessage("Expecting %s not to be empty but it was", actual)
        .isFalse();
    return this;
  }

  public TokenRangeAssert<V, T> isWrappedAround() {
    assertThat(actual.isWrappedAround())
        .overridingErrorMessage("Expecting %s to wrap around but it did not", actual)
        .isTrue();
    TokenFactory<V, T> factory = actual.tokenFactory();
    List<TokenRange<V, T>> unwrapped = actual.unwrap();
    assertThat(unwrapped.size())
        .overridingErrorMessage(
            "%s should unwrap to two ranges, but unwrapped to %s", actual, unwrapped)
        .isEqualTo(2);
    Iterator<TokenRange<V, T>> unwrappedIt = unwrapped.iterator();
    TokenRange<V, T> firstRange = unwrappedIt.next();
    assertThat(firstRange).endsWith(factory.minToken());
    TokenRange<V, T> secondRange = unwrappedIt.next();
    assertThat(secondRange).startsWith(factory.minToken());
    return this;
  }

  public TokenRangeAssert<V, T> isNotWrappedAround() {
    assertThat(actual.isWrappedAround())
        .overridingErrorMessage("Expecting %s to not wrap around but it did", actual)
        .isFalse();
    assertThat(actual.unwrap()).containsExactly(actual);
    return this;
  }

  @SafeVarargs
  public final TokenRangeAssert<V, T> unwrapsTo(TokenRange<V, T>... subRanges) {
    assertThat(actual.unwrap()).containsExactly(subRanges);
    return this;
  }
}
