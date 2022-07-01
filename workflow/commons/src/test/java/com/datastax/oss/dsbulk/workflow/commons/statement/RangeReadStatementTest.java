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
package com.datastax.oss.dsbulk.workflow.commons.statement;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenRange;
import java.net.URI;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class RangeReadStatementTest {

  @Test
  void rangeReadResource() {}

  @ParameterizedTest
  @MethodSource
  void should_create_range_read_resource(
      CqlIdentifier keyspaceName, CqlIdentifier tableName, TokenRange range, URI uri) {
    assertThat(RangeReadStatement.rangeReadResource(keyspaceName, tableName, range)).isEqualTo(uri);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_create_range_read_resource() {
    return Stream.of(
        Arguments.of(
            CqlIdentifier.fromInternal("ks1"),
            CqlIdentifier.fromInternal("table1"),
            new Murmur3TokenRange(new Murmur3Token(1), new Murmur3Token(2)),
            URI.create("cql://ks1/table1?start=1&end=2")),
        Arguments.of(
            CqlIdentifier.fromInternal("MY_KEYSPACE_1"),
            CqlIdentifier.fromInternal("MY_TABLE_1"),
            new Murmur3TokenRange(new Murmur3Token(1), new Murmur3Token(2)),
            URI.create("cql://MY_KEYSPACE_1/MY_TABLE_1?start=1&end=2")));
  }
}
