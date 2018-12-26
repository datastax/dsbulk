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
import static com.datastax.driver.core.DriverCoreCommonsTestHooks.newToken;
import static com.datastax.driver.core.DriverCoreCommonsTestHooks.newTokenRange;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.global;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.hosts;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.partitions;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.ranges;
import static java.net.InetSocketAddress.createUnresolved;
import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Sets.newLinkedHashSet;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptor;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.google.common.collect.Sets;
import com.typesafe.config.ConfigFactory;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@ExtendWith(StreamInterceptingExtension.class)
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
  @Mock private Row row4;
  @Mock private Row row5;
  @Mock private Row row6;
  @Mock private Row row7;
  @Mock private Row row8;
  @Mock private Row row9;
  @Mock private Row row10;

  @Mock private ReadResult result1;
  @Mock private ReadResult result2;
  @Mock private ReadResult result3;
  @Mock private ReadResult result4;
  @Mock private ReadResult result5;
  @Mock private ReadResult result6;
  @Mock private ReadResult result7;
  @Mock private ReadResult result8;
  @Mock private ReadResult result9;
  @Mock private ReadResult result10;

  private ByteBuffer bb1 = ByteBuffer.wrap(new byte[] {0, 0, 0, 1});
  private ByteBuffer bb2 = ByteBuffer.wrap(new byte[] {0, 0, 0, 2});
  private ByteBuffer bb3 = ByteBuffer.wrap(new byte[] {0, 0, 0, 3});
  private ByteBuffer bb4 = ByteBuffer.wrap(new byte[] {0, 0, 0, 4});
  private ByteBuffer bb5 = ByteBuffer.wrap(new byte[] {0, 0, 0, 5});
  private ByteBuffer bb6 = ByteBuffer.wrap(new byte[] {0, 0, 0, 6});
  private ByteBuffer bb7 = ByteBuffer.wrap(new byte[] {0, 0, 0, 7});
  private ByteBuffer bb8 = ByteBuffer.wrap(new byte[] {0, 0, 0, 8});
  private ByteBuffer bb9 = ByteBuffer.wrap(new byte[] {0, 0, 0, 9});
  private ByteBuffer bb10 = ByteBuffer.wrap(new byte[] {0, 0, 0, 10});

  private ExtendedCodecRegistry codecRegistry;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    MockitoAnnotations.initMocks(this);
    when(metadata.getTokenRanges()).thenReturn(tokenRanges);
    when(metadata.getAllHosts()).thenReturn(newLinkedHashSet(host1, host2, host3));
    when(metadata.getReplicas("ks", range1)).thenReturn(singleton(host1));
    when(metadata.getReplicas("ks", range2)).thenReturn(singleton(host2));
    when(metadata.getReplicas("ks", range3)).thenReturn(singleton(host3));
    when(result1.getRow()).thenReturn(Optional.of(row1));
    when(result2.getRow()).thenReturn(Optional.of(row2));
    when(result3.getRow()).thenReturn(Optional.of(row3));
    when(result4.getRow()).thenReturn(Optional.of(row4));
    when(result5.getRow()).thenReturn(Optional.of(row5));
    when(result6.getRow()).thenReturn(Optional.of(row6));
    when(result7.getRow()).thenReturn(Optional.of(row7));
    when(result8.getRow()).thenReturn(Optional.of(row8));
    when(result9.getRow()).thenReturn(Optional.of(row9));
    when(result10.getRow()).thenReturn(Optional.of(row10));
    ColumnDefinitions definitions = newColumnDefinitions(newDefinition("pk", cint()));
    when(row1.getColumnDefinitions()).thenReturn(definitions);
    when(row2.getColumnDefinitions()).thenReturn(definitions);
    when(row3.getColumnDefinitions()).thenReturn(definitions);
    when(row4.getColumnDefinitions()).thenReturn(definitions);
    when(row5.getColumnDefinitions()).thenReturn(definitions);
    when(row6.getColumnDefinitions()).thenReturn(definitions);
    when(row7.getColumnDefinitions()).thenReturn(definitions);
    when(row8.getColumnDefinitions()).thenReturn(definitions);
    when(row9.getColumnDefinitions()).thenReturn(definitions);
    when(row10.getColumnDefinitions()).thenReturn(definitions);
    when(row1.getBytesUnsafe(0)).thenReturn(bb1);
    when(row2.getBytesUnsafe(0)).thenReturn(bb2);
    when(row3.getBytesUnsafe(0)).thenReturn(bb3);
    when(row4.getBytesUnsafe(0)).thenReturn(bb4);
    when(row5.getBytesUnsafe(0)).thenReturn(bb5);
    when(row6.getBytesUnsafe(0)).thenReturn(bb6);
    when(row7.getBytesUnsafe(0)).thenReturn(bb7);
    when(row8.getBytesUnsafe(0)).thenReturn(bb8);
    when(row9.getBytesUnsafe(0)).thenReturn(bb9);
    when(row10.getBytesUnsafe(0)).thenReturn(bb10);
    when(row1.getToken(0)).thenReturn(token1a);
    when(row2.getToken(0)).thenReturn(token2a);
    when(row3.getToken(0)).thenReturn(token3);
    when(row4.getToken(0)).thenReturn(token1a);
    when(row5.getToken(0)).thenReturn(token2a);
    when(row6.getToken(0)).thenReturn(token3);
    when(row7.getToken(0)).thenReturn(token1a);
    when(row8.getToken(0)).thenReturn(token2a);
    when(row9.getToken(0)).thenReturn(token3);
    when(row10.getToken(0)).thenReturn(token1a);
    when(metadata.newToken(bb1)).thenReturn(token1a);
    when(metadata.newToken(bb2)).thenReturn(token2a);
    when(metadata.newToken(bb3)).thenReturn(token3); // token happens to be a boundary token
    when(metadata.newToken(bb4)).thenReturn(token1a);
    when(metadata.newToken(bb5)).thenReturn(token2a);
    when(metadata.newToken(bb6)).thenReturn(token3); // token happens to be a boundary token
    when(metadata.newToken(bb7)).thenReturn(token1a);
    when(metadata.newToken(bb8)).thenReturn(token2a);
    when(metadata.newToken(bb9)).thenReturn(token3); // token happens to be a boundary token
    when(metadata.newToken(bb10)).thenReturn(token1a); // token happens to be a boundary token
    when(host1.getSocketAddress()).thenReturn(createUnresolved("host1.com", 9042));
    when(host2.getSocketAddress()).thenReturn(createUnresolved("host2.com", 9042));
    when(host3.getSocketAddress()).thenReturn(createUnresolved("host3.com", 9042));
    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.codec"));
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    this.codecRegistry = settings.createCodecRegistry(new CodecRegistry(), false, false);
  }

  @Test
  void should_count_total_rows(StreamInterceptor stdout) {
    DefaultReadResultCounter counter =
        new DefaultReadResultCounter("ks", metadata, EnumSet.of(global), 10, V4, codecRegistry);

    counter.newCountingUnit().update(result1);
    counter.consolidateUnitCounts();
    assertThat(counter.totalRows).isOne();

    counter.newCountingUnit().update(result2);
    counter.consolidateUnitCounts();
    assertThat(counter.totalRows).isEqualTo(2);

    counter.reportTotals();
    assertThat(stdout.getStreamLines()).contains("2");
  }

  @Test
  void should_count_hosts(StreamInterceptor stdout) {
    DefaultReadResultCounter counter =
        new DefaultReadResultCounter("ks", metadata, EnumSet.of(hosts), 10, V4, codecRegistry);

    ReadResultCounter.CountingUnit unit = counter.newCountingUnit();

    // add token1a, belongs to range1/host1
    unit.update(result1);
    counter.consolidateUnitCounts();
    assertThat(counter.totalRows).isOne();
    assertThat(counter.totalsByHost)
        .containsEntry(host1.getSocketAddress(), 1L)
        .doesNotContainKey(host2.getSocketAddress())
        .doesNotContainKey(host3.getSocketAddress());

    // add token2a, belongs to range2/host2
    unit.update(result2);
    counter.consolidateUnitCounts();
    assertThat(counter.totalRows).isEqualTo(2);
    assertThat(counter.totalsByHost)
        .containsEntry(host1.getSocketAddress(), 1L)
        .containsEntry(host2.getSocketAddress(), 1L)
        .doesNotContainKey(host3.getSocketAddress());

    // add token3, belongs to range2/host2 (its the range's end token)
    unit.update(result3);
    counter.consolidateUnitCounts();
    assertThat(counter.totalRows).isEqualTo(3);
    assertThat(counter.totalsByHost)
        .containsEntry(host1.getSocketAddress(), 1L)
        .containsEntry(host2.getSocketAddress(), 2L)
        .doesNotContainKey(host3.getSocketAddress());

    counter.consolidateUnitCounts();
    counter.reportTotals();
    assertThat(stdout.getStreamLines())
        .contains(
            String.format("%s 1 33.33", host1.getSocketAddress()),
            String.format("%s 2 66.67", host2.getSocketAddress()),
            String.format("%s 0 0.00", host3.getSocketAddress()));
  }

  @Test
  void should_count_ranges(StreamInterceptor stdout) {
    DefaultReadResultCounter counter =
        new DefaultReadResultCounter("ks", metadata, EnumSet.of(ranges), 10, V4, codecRegistry);

    ReadResultCounter.CountingUnit unit = counter.newCountingUnit();

    // add token1a, belongs to range1/host1
    unit.update(result1);
    counter.consolidateUnitCounts();
    assertThat(counter.totalRows).isOne();
    assertThat(counter.totalsByRange)
        .containsEntry(range1, 1L)
        .doesNotContainKey(range2)
        .doesNotContainKey(range3);

    // add token2a, belongs to range2/host2
    unit.update(result2);
    counter.consolidateUnitCounts();
    assertThat(counter.totalRows).isEqualTo(2);
    assertThat(counter.totalsByRange)
        .containsEntry(range1, 1L)
        .containsEntry(range2, 1L)
        .doesNotContainKey(range3);

    // add token3, belongs to range2/host2 (its the range's end token)
    unit.update(result3);
    counter.consolidateUnitCounts();
    assertThat(counter.totalRows).isEqualTo(3);
    assertThat(counter.totalsByRange)
        .containsEntry(range1, 1L)
        .containsEntry(range2, 2L)
        .doesNotContainKey(range3);

    counter.consolidateUnitCounts();
    counter.reportTotals();

    assertThat(stdout.getStreamLines())
        .contains(
            String.format("%s %s 1 33.33", range1.getStart(), range1.getEnd()),
            String.format("%s %s 2 66.67", range2.getStart(), range2.getEnd()),
            String.format("%s %s 0 0.00", range3.getStart(), range3.getEnd()));
  }

  @Test
  void should_count_biggest_partitions(StreamInterceptor stdout) {
    DefaultReadResultCounter counter =
        new DefaultReadResultCounter("ks", metadata, EnumSet.of(partitions), 3, V4, codecRegistry);

    DefaultReadResultCounter.DefaultCountingUnit unit = counter.newCountingUnit();

    unit.update(result1);

    assertThat(unit.totalsByPartitionKey.size()).isZero();
    assertThat(unit.currentPkCount).isOne();
    assertThat(unit.currentPk.components).containsOnly(bb1);
    assertThat(unit.currentPk.hashCode).isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb1}));
    assertThat(unit.totalsByPartitionKey).isEmpty();

    unit.update(result2);

    // should store pk1=1 in pkcs[0]
    // pkcs now should be [pk1=1]
    // current pk now pk2=1
    assertThat(unit.currentPkCount).isOne();
    assertThat(unit.currentPk.components).containsOnly(bb2);
    assertThat(unit.currentPk.hashCode).isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb2}));
    assertThat(unit.totalsByPartitionKey.size()).isOne();
    assertThat(unit.totalsByPartitionKey.get(0).count).isOne();
    assertThat(unit.totalsByPartitionKey.get(0).pk.components).containsOnly(bb1);
    assertThat(unit.totalsByPartitionKey.get(0).pk.hashCode)
        .isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb1}));

    unit.update(result2);

    // current pk still pk2=2
    assertThat(unit.currentPkCount).isEqualTo(2);
    assertThat(unit.currentPk.components).containsOnly(bb2);
    assertThat(unit.currentPk.hashCode).isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb2}));
    assertThat(unit.totalsByPartitionKey.size()).isOne();
    assertThat(unit.totalsByPartitionKey.get(0).count).isOne();
    assertThat(unit.totalsByPartitionKey.get(0).pk.components).containsOnly(bb1);
    assertThat(unit.totalsByPartitionKey.get(0).pk.hashCode)
        .isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb1}));

    unit.update(result3);

    // should store pk2=2 in pkcs[1]
    // pkcs now should be [pk1=1,pk2=2]
    // current pk now pk3=1
    assertThat(unit.currentPkCount).isEqualTo(1);
    assertThat(unit.currentPk.components).containsOnly(bb3);
    assertThat(unit.currentPk.hashCode).isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb3}));
    assertThat(unit.totalsByPartitionKey.size()).isEqualTo(2);
    assertThat(unit.totalsByPartitionKey.get(0).count).isOne();
    assertThat(unit.totalsByPartitionKey.get(0).pk.components).containsOnly(bb1);
    assertThat(unit.totalsByPartitionKey.get(0).pk.hashCode)
        .isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb1}));
    assertThat(unit.totalsByPartitionKey.get(1).count).isEqualTo(2);
    assertThat(unit.totalsByPartitionKey.get(1).pk.components).containsOnly(bb2);
    assertThat(unit.totalsByPartitionKey.get(1).pk.hashCode)
        .isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb2}));

    unit.update(result4);

    // should store pk3=1 in pkcs[0] or pkcs[1], pk2=2 in pkcs[2]
    // pkcs now should be [?=1,?=1,pk2=2]
    // current pk now pk4=1
    assertThat(unit.currentPkCount).isEqualTo(1);
    assertThat(unit.currentPk.components).containsOnly(bb4);
    assertThat(unit.currentPk.hashCode).isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb4}));
    assertThat(unit.totalsByPartitionKey.size()).isEqualTo(3);
    assertThat(unit.totalsByPartitionKey.get(0).count).isEqualTo(1);
    assertThat(unit.totalsByPartitionKey.get(1).count).isEqualTo(1);
    assertThat(unit.totalsByPartitionKey.get(2).count).isEqualTo(2);
    assertThat(unit.totalsByPartitionKey.get(2).pk.components).containsOnly(bb2);
    assertThat(unit.totalsByPartitionKey.get(2).pk.hashCode)
        .isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb2}));

    unit.update(result5);

    // should not store pk4=1 since size=max and currentPkCount=totalsByPartitionKey.get(0).count
    // current pk now pk5=1
    assertThat(unit.currentPkCount).isEqualTo(1);
    assertThat(unit.currentPk.components).containsOnly(bb5);
    assertThat(unit.currentPk.hashCode).isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb5}));
    assertThat(unit.totalsByPartitionKey.size()).isEqualTo(3);
    assertThat(unit.totalsByPartitionKey.get(0).count).isEqualTo(1);
    assertThat(unit.totalsByPartitionKey.get(1).count).isEqualTo(1);
    assertThat(unit.totalsByPartitionKey.get(2).count).isEqualTo(2);
    assertThat(unit.totalsByPartitionKey.get(2).pk.components).containsOnly(bb2);
    assertThat(unit.totalsByPartitionKey.get(2).pk.hashCode)
        .isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb2}));

    unit.update(result5);

    // current pk still pk5=2
    assertThat(unit.currentPkCount).isEqualTo(2);
    assertThat(unit.currentPk.components).containsOnly(bb5);
    assertThat(unit.currentPk.hashCode).isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb5}));
    assertThat(unit.totalsByPartitionKey.size()).isEqualTo(3);
    assertThat(unit.totalsByPartitionKey.get(0).count).isEqualTo(1);
    assertThat(unit.totalsByPartitionKey.get(1).count).isEqualTo(1);
    assertThat(unit.totalsByPartitionKey.get(2).count).isEqualTo(2);
    assertThat(unit.totalsByPartitionKey.get(2).pk.components).containsOnly(bb2);
    assertThat(unit.totalsByPartitionKey.get(2).pk.hashCode)
        .isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb2}));

    unit.update(result6);

    // pkcs now should be [?=1,pk5=2,pk2=2] (shift left)
    // current pk now pk6=1
    assertThat(unit.currentPkCount).isEqualTo(1);
    assertThat(unit.currentPk.components).containsOnly(bb6);
    assertThat(unit.currentPk.hashCode).isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb6}));
    assertThat(unit.totalsByPartitionKey.size()).isEqualTo(3);
    assertThat(unit.totalsByPartitionKey.get(0).count).isEqualTo(1);
    assertThat(unit.totalsByPartitionKey.get(1).count).isEqualTo(2);
    assertThat(unit.totalsByPartitionKey.get(1).pk.components).containsOnly(bb5);
    assertThat(unit.totalsByPartitionKey.get(1).pk.hashCode)
        .isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb5}));
    assertThat(unit.totalsByPartitionKey.get(2).count).isEqualTo(2);
    assertThat(unit.totalsByPartitionKey.get(2).pk.components).containsOnly(bb2);
    assertThat(unit.totalsByPartitionKey.get(2).pk.hashCode)
        .isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb2}));

    unit.update(result6);
    assertThat(unit.currentPkCount).isEqualTo(2);
    unit.update(result6);
    assertThat(unit.currentPkCount).isEqualTo(3);

    unit.update(result7);

    // pkcs now should be [pk5=2,pk2=2,pk6=3] (shift left)
    // current pk now pk7=1
    assertThat(unit.currentPkCount).isEqualTo(1);
    assertThat(unit.currentPk.components).containsOnly(bb7);
    assertThat(unit.currentPk.hashCode).isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb7}));
    assertThat(unit.totalsByPartitionKey.size()).isEqualTo(3);
    assertThat(unit.totalsByPartitionKey.get(0).count).isEqualTo(2);
    assertThat(unit.totalsByPartitionKey.get(0).pk.components).containsOnly(bb5);
    assertThat(unit.totalsByPartitionKey.get(0).pk.hashCode)
        .isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb5}));
    assertThat(unit.totalsByPartitionKey.get(1).count).isEqualTo(2);
    assertThat(unit.totalsByPartitionKey.get(1).pk.components).containsOnly(bb2);
    assertThat(unit.totalsByPartitionKey.get(1).pk.hashCode)
        .isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb2}));
    assertThat(unit.totalsByPartitionKey.get(2).count).isEqualTo(3);
    assertThat(unit.totalsByPartitionKey.get(2).pk.components).containsOnly(bb6);
    assertThat(unit.totalsByPartitionKey.get(2).pk.hashCode)
        .isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb6}));

    unit.update(result7);
    assertThat(unit.currentPkCount).isEqualTo(2);
    unit.update(result7);
    assertThat(unit.currentPkCount).isEqualTo(3);

    unit.update(result8);

    // pkcs now should be [pk2=2,pk7=3,pk6=3] (shift left)
    // current pk now pk8=1
    assertThat(unit.currentPkCount).isEqualTo(1);
    assertThat(unit.currentPk.components).containsOnly(bb8);
    assertThat(unit.currentPk.hashCode).isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb8}));
    assertThat(unit.totalsByPartitionKey.size()).isEqualTo(3);
    assertThat(unit.totalsByPartitionKey.get(0).count).isEqualTo(2);
    assertThat(unit.totalsByPartitionKey.get(0).pk.components).containsOnly(bb2);
    assertThat(unit.totalsByPartitionKey.get(0).pk.hashCode)
        .isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb2}));
    assertThat(unit.totalsByPartitionKey.get(1).count).isEqualTo(3);
    assertThat(unit.totalsByPartitionKey.get(1).pk.components).containsOnly(bb7);
    assertThat(unit.totalsByPartitionKey.get(1).pk.hashCode)
        .isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb7}));
    assertThat(unit.totalsByPartitionKey.get(2).count).isEqualTo(3);
    assertThat(unit.totalsByPartitionKey.get(2).pk.components).containsOnly(bb6);
    assertThat(unit.totalsByPartitionKey.get(2).pk.hashCode)
        .isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb6}));

    unit.update(result8);
    unit.update(result8);
    unit.update(result8);
    assertThat(unit.currentPkCount).isEqualTo(4);

    // [pk7=3,pk6=3,pk8=4]

    unit.update(result9);
    unit.update(result9);
    unit.update(result9);
    unit.update(result9);
    unit.update(result9);
    assertThat(unit.currentPkCount).isEqualTo(5);

    // [pk6=3,pk8=4,pk9=5]

    unit.update(result10);
    unit.update(result10);
    unit.update(result10);
    unit.update(result10);
    assertThat(unit.currentPkCount).isEqualTo(4);

    // simulate end of rows
    counter.close();

    // [pk10=4,pk8=4,pk9=5]
    assertThat(unit.totalsByPartitionKey.size()).isEqualTo(3);
    assertThat(unit.totalsByPartitionKey.get(0).count).isEqualTo(4);
    assertThat(unit.totalsByPartitionKey.get(0).pk.components).containsOnly(bb10);
    assertThat(unit.totalsByPartitionKey.get(0).pk.hashCode)
        .isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb10}));
    assertThat(unit.totalsByPartitionKey.get(1).count).isEqualTo(4);
    assertThat(unit.totalsByPartitionKey.get(1).pk.components).containsOnly(bb8);
    assertThat(unit.totalsByPartitionKey.get(1).pk.hashCode)
        .isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb8}));
    assertThat(unit.totalsByPartitionKey.get(2).count).isEqualTo(5);
    assertThat(unit.totalsByPartitionKey.get(2).pk.components).containsOnly(bb9);
    assertThat(unit.totalsByPartitionKey.get(2).pk.hashCode)
        .isEqualTo(Arrays.hashCode(new ByteBuffer[] {bb9}));

    List<DefaultReadResultCounter.PartitionKeyCount> totals = counter.totalsByPartitionKey;

    assertThat(totals.get(0).count).isEqualTo(5);
    assertThat(totals.get(1).count).isEqualTo(4);
    assertThat(totals.get(2).count).isEqualTo(4);

    counter.reportTotals();

    // should retain pk8=4, pk10=4, pk9=5
    // total rows is 26, so 4 rows is 100*4/26 = 15.38%
    // total rows is 26, so 5 rows is 100*5/26 = 19.23%
    assertThat(stdout.getStreamLines()).contains("8 4 15.38", "9 5 19.23", "10 4 15.38");
  }

  @Test
  void should_count_biggest_partitions_multi_threaded(StreamInterceptor stdout)
      throws InterruptedException {
    DefaultReadResultCounter counter =
        new DefaultReadResultCounter("ks", metadata, EnumSet.of(partitions), 3, V4, codecRegistry);

    DefaultReadResultCounter.DefaultCountingUnit unit1 = counter.newCountingUnit();
    DefaultReadResultCounter.DefaultCountingUnit unit2 = counter.newCountingUnit();
    DefaultReadResultCounter.DefaultCountingUnit unit3 = counter.newCountingUnit();
    DefaultReadResultCounter.DefaultCountingUnit unit4 = counter.newCountingUnit();

    Thread t1 =
        new Thread(
            () -> {
              for (int i = 0; i < 10; i++) {
                unit1.update(result1);
              }
              for (int i = 0; i < 9; i++) {
                unit1.update(result2);
              }
              for (int i = 0; i < 8; i++) {
                unit1.update(result3);
              }
            });

    Thread t2 =
        new Thread(
            () -> {
              for (int i = 0; i < 8; i++) {
                unit2.update(result4);
              }
              for (int i = 0; i < 9; i++) {
                unit2.update(result5);
              }
              for (int i = 0; i < 10; i++) {
                unit2.update(result6);
              }
            });

    Thread t3 =
        new Thread(
            () -> {
              for (int i = 0; i < 5; i++) {
                unit3.update(result7);
              }
              for (int i = 0; i < 5; i++) {
                unit3.update(result8);
              }
              for (int i = 0; i < 5; i++) {
                unit3.update(result9);
              }
            });

    Thread t4 =
        new Thread(
            () -> {
              for (int i = 0; i < 10; i++) {
                unit4.update(result10);
              }
            });

    t1.start();
    t2.start();
    t3.start();
    t4.start();

    t1.join();
    t2.join();
    t3.join();
    t4.join();

    counter.close();

    assertThat(unit1.totalsByPartitionKey.size()).isEqualTo(3);
    assertThat(unit1.totalsByPartitionKey.get(0).count).isEqualTo(8);
    assertThat(unit1.totalsByPartitionKey.get(0).pk.components).containsOnly(bb3);
    assertThat(unit1.totalsByPartitionKey.get(1).count).isEqualTo(9);
    assertThat(unit1.totalsByPartitionKey.get(1).pk.components).containsOnly(bb2);
    assertThat(unit1.totalsByPartitionKey.get(2).count).isEqualTo(10);
    assertThat(unit1.totalsByPartitionKey.get(2).pk.components).containsOnly(bb1);

    assertThat(unit2.totalsByPartitionKey.size()).isEqualTo(3);
    assertThat(unit2.totalsByPartitionKey.get(0).count).isEqualTo(8);
    assertThat(unit2.totalsByPartitionKey.get(0).pk.components).containsOnly(bb4);
    assertThat(unit2.totalsByPartitionKey.get(1).count).isEqualTo(9);
    assertThat(unit2.totalsByPartitionKey.get(1).pk.components).containsOnly(bb5);
    assertThat(unit2.totalsByPartitionKey.get(2).count).isEqualTo(10);
    assertThat(unit2.totalsByPartitionKey.get(2).pk.components).containsOnly(bb6);

    assertThat(unit3.totalsByPartitionKey.size()).isEqualTo(3);
    assertThat(unit3.totalsByPartitionKey.get(0).count).isEqualTo(5);
    assertThat(unit3.totalsByPartitionKey.get(1).count).isEqualTo(5);
    assertThat(unit3.totalsByPartitionKey.get(2).count).isEqualTo(5);

    assertThat(unit4.totalsByPartitionKey.size()).isEqualTo(1);
    assertThat(unit4.totalsByPartitionKey.get(0).count).isEqualTo(10);
    assertThat(unit4.totalsByPartitionKey.get(0).pk.components).containsOnly(bb10);

    List<DefaultReadResultCounter.PartitionKeyCount> totals = counter.totalsByPartitionKey;

    assertThat(totals.get(0).count).isEqualTo(10);
    assertThat(totals.get(1).count).isEqualTo(10);
    assertThat(totals.get(2).count).isEqualTo(10);

    counter.reportTotals();

    // should retain pk1=10, pk6=10, pk10=10
    // total rows is 79, so 10 rows is 100*10/79 = 12.66%
    assertThat(stdout.getStreamLines()).contains("1 10 12.66", "6 10 12.66", "10 10 12.66");
  }
}
