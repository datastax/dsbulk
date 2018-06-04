/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DriverCoreCommonsTestHooks.newColumnDefinitions;
import static com.datastax.driver.core.DriverCoreCommonsTestHooks.newDefinition;
import static com.datastax.driver.core.DriverCoreEngineTestHooks.newToken;
import static com.datastax.driver.core.DriverCoreEngineTestHooks.newTokenRange;
import static com.datastax.dsbulk.engine.internal.settings.SchemaSettings.StatisticsMode.all;
import static java.net.InetSocketAddress.createUnresolved;
import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class DefaultReadResultCounterTest {

  private final Token token1 = newToken(-9223372036854775808L);
  private final Token token2 = newToken(-3074457345618258603L);
  private final Token token3 = newToken(3074457345618258602L);

  // a random token inside range1
  private final Token token1a = newToken(-9223372036854775808L + 1);
  // a random token inside range2
  private final Token token2a = newToken(-3074457345618258603L + 1);

  private final TokenRange range1 = newTokenRange(token1, token2);
  private final TokenRange range2 = newTokenRange(token2, token3);
  private final TokenRange range3 = newTokenRange(token3, token1);

  private final Set<TokenRange> tokenRanges = Sets.newHashSet(range1, range2, range3);

  @Mock private Metadata metadata;

  @Mock private Host host1;
  @Mock private Host host2;
  @Mock private Host host3;

  @Mock private Row row1;
  @Mock private Row row2;
  @Mock private Row row3;

  @Mock private ReadResult result1;
  @Mock private ReadResult result2;
  @Mock private ReadResult result3;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    MockitoAnnotations.initMocks(this);
    when(metadata.getTokenRanges()).thenReturn(tokenRanges);
    when(metadata.getReplicas("ks", range1)).thenReturn(singleton(host1));
    when(metadata.getReplicas("ks", range2)).thenReturn(singleton(host2));
    when(metadata.getReplicas("ks", range3)).thenReturn(singleton(host3));
    when(result1.getRow()).thenReturn(Optional.of(row1));
    when(result2.getRow()).thenReturn(Optional.of(row2));
    when(result3.getRow()).thenReturn(Optional.of(row3));
    ColumnDefinitions definitions = newColumnDefinitions(newDefinition("pk", cint()));
    when(row1.getColumnDefinitions()).thenReturn(definitions);
    when(row2.getColumnDefinitions()).thenReturn(definitions);
    when(row3.getColumnDefinitions()).thenReturn(definitions);
    ByteBuffer bb1 = ByteBuffer.wrap(new byte[] {1});
    ByteBuffer bb2 = ByteBuffer.wrap(new byte[] {2});
    ByteBuffer bb3 = ByteBuffer.wrap(new byte[] {3});
    when(row1.getBytesUnsafe(0)).thenReturn(bb1);
    when(row2.getBytesUnsafe(0)).thenReturn(bb2);
    when(row3.getBytesUnsafe(0)).thenReturn(bb3);
    when(metadata.newToken(bb1)).thenReturn(token1a);
    when(metadata.newToken(bb2)).thenReturn(token2a);
    when(metadata.newToken(bb3)).thenReturn(token3); // token happens to be a boundary token
    when(host1.getSocketAddress()).thenReturn(createUnresolved("host1.com", 9042));
    when(host2.getSocketAddress()).thenReturn(createUnresolved("host2.com", 9042));
    when(host3.getSocketAddress()).thenReturn(createUnresolved("host3.com", 9042));
  }

  @Test
  void should_map_result_to_mapped_record_when_mapping_succeeds() {
    DefaultReadResultCounter counter = new DefaultReadResultCounter("ks", metadata, all);

    // add token1a, belongs to range1/host1
    counter.update(result1);
    assertThat(counter.total.sum()).isOne();
    Map<TokenRange, Long> ranges = Maps.transformValues(counter.totalsByRange, LongAdder::sum);
    Map<InetSocketAddress, Long> hosts = Maps.transformValues(counter.totalsByHost, LongAdder::sum);
    assertThat(ranges)
        .containsEntry(range1, 1L)
        .doesNotContainKey(range2)
        .doesNotContainKey(range3);
    assertThat(hosts)
        .containsEntry(host1.getSocketAddress(), 1L)
        .doesNotContainKey(host2.getSocketAddress())
        .doesNotContainKey(host3.getSocketAddress());

    // add token2a, belongs to range2/host2
    counter.update(result2);
    assertThat(counter.total.sum()).isEqualTo(2);
    ranges = Maps.transformValues(counter.totalsByRange, LongAdder::sum);
    hosts = Maps.transformValues(counter.totalsByHost, LongAdder::sum);
    assertThat(ranges)
        .containsEntry(range1, 1L)
        .containsEntry(range2, 1L)
        .doesNotContainKey(range3);
    assertThat(hosts)
        .containsEntry(host1.getSocketAddress(), 1L)
        .containsEntry(host2.getSocketAddress(), 1L)
        .doesNotContainKey(host3.getSocketAddress());

    // add token3, belongs to range2/host2 (its the range's end token)
    counter.update(result3);
    assertThat(counter.total.sum()).isEqualTo(3);
    ranges = Maps.transformValues(counter.totalsByRange, LongAdder::sum);
    hosts = Maps.transformValues(counter.totalsByHost, LongAdder::sum);
    assertThat(ranges)
        .containsEntry(range1, 1L)
        .containsEntry(range2, 2L)
        .doesNotContainKey(range3);
    assertThat(hosts)
        .containsEntry(host1.getSocketAddress(), 1L)
        .containsEntry(host2.getSocketAddress(), 2L)
        .doesNotContainKey(host3.getSocketAddress());
  }
}
