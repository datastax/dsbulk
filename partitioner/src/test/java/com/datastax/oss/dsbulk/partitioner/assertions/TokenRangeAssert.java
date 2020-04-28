/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.partitioner.assertions;

import static com.datastax.oss.dsbulk.partitioner.assertions.PartitionerAssertions.assertThat;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.dsbulk.commons.utils.TokenUtils;
import com.datastax.oss.dsbulk.partitioner.BulkTokenFactory;
import com.datastax.oss.dsbulk.partitioner.BulkTokenRange;
import com.datastax.oss.dsbulk.partitioner.murmur3.Murmur3BulkTokenFactory;
import com.datastax.oss.dsbulk.partitioner.murmur3.Murmur3BulkTokenRange;
import com.datastax.oss.dsbulk.partitioner.random.RandomBulkTokenFactory;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.data.Offset;

@SuppressWarnings("UnusedReturnValue")
public class TokenRangeAssert extends AbstractObjectAssert<TokenRangeAssert, BulkTokenRange> {

  TokenRangeAssert(BulkTokenRange actual) {
    super(actual, TokenRangeAssert.class);
  }

  public TokenRangeAssert startsWith(Object value) {
    assertThat(TokenUtils.getTokenValue(actual.getStart()))
        .overridingErrorMessage(
            "Expecting %s to start with %s but it starts with %s",
            actual, value, TokenUtils.getTokenValue(actual.getStart()))
        .isEqualTo(value);
    return this;
  }

  public TokenRangeAssert endsWith(Object value) {
    assertThat(TokenUtils.getTokenValue(actual.getEnd()))
        .overridingErrorMessage(
            "Expecting %s to end with %s but it ends with %s",
            actual, value, TokenUtils.getTokenValue(actual.getEnd()))
        .isEqualTo(value);
    return this;
  }

  public TokenRangeAssert startsWith(Token token) {
    assertThat(actual.getStart())
        .overridingErrorMessage(
            "Expecting %s to start with %s but it starts with %s",
            actual, token, TokenUtils.getTokenValue(actual.getStart()))
        .isEqualTo(token);
    return this;
  }

  public TokenRangeAssert endsWith(Token token) {
    assertThat(actual.getEnd())
        .overridingErrorMessage(
            "Expecting %s to end with %s but it ends with %s",
            actual, token, TokenUtils.getTokenValue(actual.getEnd()))
        .isEqualTo(token);
    return this;
  }

  public TokenRangeAssert hasRange(Object start, Object end) {
    assertThat(TokenUtils.getTokenValue(actual.getStart()))
        .overridingErrorMessage(
            "Expecting %s to start with %s but it starts with %s",
            actual, start, TokenUtils.getTokenValue(actual.getStart()))
        .isEqualTo(start);
    assertThat(TokenUtils.getTokenValue(actual.getEnd()))
        .overridingErrorMessage(
            "Expecting %s to end with %s but it ends with %s",
            actual, end, TokenUtils.getTokenValue(actual.getStart()))
        .isEqualTo(end);
    return this;
  }

  public TokenRangeAssert hasSize(long size) {
    assertThat(actual.size())
        .overridingErrorMessage(
            "Expecting %s to have size %d but it has size %d", actual, size, actual.size())
        .isEqualTo(size);
    return this;
  }

  public TokenRangeAssert hasSize(BigInteger size) {
    assertThat(actual.size())
        .overridingErrorMessage(
            "Expecting %s to have size %d but it has size %d", actual, size, actual.size())
        .isEqualTo(size);
    return this;
  }

  public TokenRangeAssert hasFraction(double fraction) {
    assertThat(actual.fraction())
        .overridingErrorMessage(
            "Expecting %s to have fraction %f but it has fraction %f",
            actual, fraction, actual.fraction())
        .isEqualTo(fraction);
    return this;
  }

  public TokenRangeAssert hasFraction(double fraction, Offset<Double> offset) {
    assertThat(actual.fraction())
        .overridingErrorMessage(
            "Expecting %s to have fraction %f (+- %s) but it has fraction %f",
            actual, fraction, offset, actual.fraction())
        .isEqualTo(fraction, offset);
    return this;
  }

  public TokenRangeAssert hasReplicas(Node... hosts) {
    Set<EndPoint> expected =
        Arrays.stream(hosts).map(Node::getEndPoint).collect(Collectors.toSet());
    assertThat(actual.replicas())
        .overridingErrorMessage(
            "Expecting %s to have replicas %s but it had %s", actual, expected, actual.replicas())
        .isEqualTo(expected);
    return this;
  }

  public TokenRangeAssert isEmpty() {
    assertThat(actual.isEmpty())
        .overridingErrorMessage("Expecting %s to be empty but it was not", actual)
        .isTrue();
    return this;
  }

  public TokenRangeAssert isNotEmpty() {
    assertThat(actual.isEmpty())
        .overridingErrorMessage("Expecting %s not to be empty but it was", actual)
        .isFalse();
    return this;
  }

  public TokenRangeAssert isWrappedAround() {
    assertThat(actual.isWrappedAround())
        .overridingErrorMessage("Expecting %s to wrap around but it did not", actual)
        .isTrue();
    BulkTokenFactory factory =
        actual instanceof Murmur3BulkTokenRange
            ? new Murmur3BulkTokenFactory()
            : new RandomBulkTokenFactory();
    List<TokenRange> unwrapped = actual.unwrap();
    assertThat(unwrapped.size())
        .overridingErrorMessage(
            "%s should unwrap to two ranges, but unwrapped to %s", actual, unwrapped)
        .isEqualTo(2);
    Iterator<TokenRange> unwrappedIt = unwrapped.iterator();
    TokenRange firstRange = unwrappedIt.next();
    assertThat(factory.range(firstRange.getStart(), firstRange.getEnd(), Collections.emptySet()))
        .endsWith(factory.minToken());
    TokenRange secondRange = unwrappedIt.next();
    assertThat(factory.range(secondRange.getStart(), secondRange.getEnd(), Collections.emptySet()))
        .startsWith(factory.minToken());
    return this;
  }

  public TokenRangeAssert isNotWrappedAround() {
    assertThat(actual.isWrappedAround())
        .overridingErrorMessage("Expecting %s to not wrap around but it did", actual)
        .isFalse();
    assertThat(actual.unwrap()).containsExactly(actual);
    return this;
  }

  public final TokenRangeAssert unwrapsTo(BulkTokenRange... subRanges) {
    assertThat(actual.unwrap()).containsExactly(subRanges);
    return this;
  }
}
