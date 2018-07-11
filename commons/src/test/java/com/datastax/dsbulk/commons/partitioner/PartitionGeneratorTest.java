/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.partitioner;

import static com.datastax.driver.core.DriverCoreCommonsTestHooks.newToken;
import static com.datastax.driver.core.DriverCoreCommonsTestHooks.newTokenRange;
import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static java.net.InetSocketAddress.createUnresolved;
import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.offset;
import static org.assertj.core.util.Sets.newLinkedHashSet;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

class PartitionGeneratorTest {

  private final Murmur3TokenFactory tokenFactory = Murmur3TokenFactory.INSTANCE;

  // A typical ring with Murmur3 and 3 nodes, single DC, SimpleStrategy:

  private final com.datastax.driver.core.TokenRange rangeS1 =
      range(-9223372036854775808L, -3074457345618258603L);

  private final com.datastax.driver.core.TokenRange rangeS2 =
      range(-3074457345618258603L, 3074457345618258602L);

  private final com.datastax.driver.core.TokenRange rangeS3 =
      range(3074457345618258602L, -9223372036854775808L);

  private Set<com.datastax.driver.core.TokenRange> singleDCRanges =
      newLinkedHashSet(rangeS1, rangeS2, rangeS3);

  // A typical ring with Murmur3 and 6 nodes, 2 DCs, NetworkTopologyStrategy:

  private final com.datastax.driver.core.TokenRange rangeM1 =
      range(-9223372036854775808L, -9223372036854775708L); // 100

  private final com.datastax.driver.core.TokenRange rangeM2 =
      range(-9223372036854775708L, -3074457345618258603L);

  private final com.datastax.driver.core.TokenRange rangeM3 =
      range(-3074457345618258603L, -3074457345618258503L); // 100

  private final com.datastax.driver.core.TokenRange rangeM4 =
      range(-3074457345618258503L, 3074457345618258602L);

  private final com.datastax.driver.core.TokenRange rangeM5 =
      range(3074457345618258602L, 3074457345618258702L); // 100

  private final com.datastax.driver.core.TokenRange rangeM6 =
      range(3074457345618258702L, -9223372036854775808L);

  private Set<com.datastax.driver.core.TokenRange> multiDCRanges =
      newLinkedHashSet(rangeM1, rangeM2, rangeM3, rangeM4, rangeM5, rangeM6);

  @Mock private KeyspaceMetadata keyspace;

  @Mock private Metadata metadata;

  @Mock private Host host1;
  @Mock private Host host2;
  @Mock private Host host3;
  @Mock private Host host4;
  @Mock private Host host5;
  @Mock private Host host6;

  @BeforeEach
  void setUp() {

    initMocks(this);

    when(keyspace.getName()).thenReturn("ks");

    when(metadata.getReplicas("ks", rangeS1)).thenReturn(singleton(host1));
    when(metadata.getReplicas("ks", rangeS2)).thenReturn(singleton(host2));
    when(metadata.getReplicas("ks", rangeS3)).thenReturn(singleton(host3));

    when(metadata.getReplicas("ks", rangeM1)).thenReturn(newLinkedHashSet(host2, host4));
    when(metadata.getReplicas("ks", rangeM2)).thenReturn(newLinkedHashSet(host2, host5));
    when(metadata.getReplicas("ks", rangeM3)).thenReturn(newLinkedHashSet(host3, host5));
    when(metadata.getReplicas("ks", rangeM4)).thenReturn(newLinkedHashSet(host3, host6));
    when(metadata.getReplicas("ks", rangeM5)).thenReturn(newLinkedHashSet(host1, host6));
    when(metadata.getReplicas("ks", rangeM6)).thenReturn(newLinkedHashSet(host1, host4));

    when(host1.getSocketAddress()).thenReturn(createUnresolved("192.168.1.1", 9042));
    when(host2.getSocketAddress()).thenReturn(createUnresolved("192.168.1.2", 9042));
    when(host3.getSocketAddress()).thenReturn(createUnresolved("192.168.1.3", 9042));
    when(host4.getSocketAddress()).thenReturn(createUnresolved("192.168.1.4", 9042));
    when(host5.getSocketAddress()).thenReturn(createUnresolved("192.168.1.5", 9042));
    when(host6.getSocketAddress()).thenReturn(createUnresolved("192.168.1.6", 9042));
  }

  @Test
  void should_split_single_dc() {

    given(metadata.getTokenRanges()).willReturn(singleDCRanges);

    PartitionGenerator<Long, Token<Long>> generator =
        new PartitionGenerator<>(keyspace, metadata, tokenFactory);
    List<TokenRange<Long, Token<Long>>> splits = generator.partition(9);

    assertThat(splits.size()).isEqualTo(9);

    // range S1 -> 3 splits
    assertThat(splits.get(0))
        .startsWith(-9223372036854775808L)
        .endsWith(-7173733806442603407L)
        .hasSize(2049638230412172401L)
        .hasReplicas(host1)
        .hasFraction(0.1111111111111111d, offset(.000000001d));
    assertThat(splits.get(1))
        .startsWith(-7173733806442603407L)
        .endsWith(-5124095576030431005L)
        .hasSize(2049638230412172402L)
        .hasReplicas(host1)
        .hasFraction(0.1111111111111111d, offset(.000000001d));
    assertThat(splits.get(2))
        .startsWith(-5124095576030431005L)
        .endsWith(-3074457345618258603L)
        .hasSize(2049638230412172402L)
        .hasReplicas(host1)
        .hasFraction(0.1111111111111111d, offset(.000000001d));

    // range S2 -> 3 splits
    assertThat(splits.get(3))
        .startsWith(-3074457345618258603L)
        .endsWith(-1024819115206086202L)
        .hasSize(2049638230412172401L)
        .hasReplicas(host2)
        .hasFraction(0.1111111111111111d, offset(.000000001d));
    assertThat(splits.get(4))
        .startsWith(-1024819115206086202L)
        .endsWith(1024819115206086200L)
        .hasSize(2049638230412172402L)
        .hasReplicas(host2)
        .hasFraction(0.1111111111111111d, offset(.000000001d));
    assertThat(splits.get(5))
        .startsWith(1024819115206086200L)
        .endsWith(3074457345618258602L)
        .hasSize(2049638230412172402L)
        .hasReplicas(host2)
        .hasFraction(0.1111111111111111d, offset(.000000001d));

    // range S3 -> 3 splits
    assertThat(splits.get(6))
        .startsWith(3074457345618258602L)
        .endsWith(5124095576030431003L)
        .hasSize(2049638230412172401L)
        .hasReplicas(host3)
        .hasFraction(0.1111111111111111d, offset(.000000001d));
    assertThat(splits.get(7))
        .startsWith(5124095576030431003L)
        .endsWith(7173733806442603405L)
        .hasSize(2049638230412172402L)
        .hasReplicas(host3)
        .hasFraction(0.1111111111111111d, offset(.000000001d));
    assertThat(splits.get(8))
        .startsWith(7173733806442603405L)
        .endsWith(-9223372036854775808L)
        .hasSize(2049638230412172402L)
        .hasReplicas(host3)
        .hasFraction(0.1111111111111111d, offset(.000000001d));

    assertThat(splits.stream().map(TokenRange::fraction).reduce(0d, (f1, f2) -> f1 + f2))
        .isEqualTo(1d, offset(.000000001));
  }

  @Test
  void should_split_multi_dc() {

    given(metadata.getTokenRanges()).willReturn(multiDCRanges);

    PartitionGenerator<Long, Token<Long>> generator =
        new PartitionGenerator<>(keyspace, metadata, tokenFactory);
    List<TokenRange<Long, Token<Long>>> splits = generator.partition(9);

    assertThat(splits.size()).isEqualTo(12);

    // range M2 -> 3 splits
    assertThat(splits.get(1))
        .startsWith(-9223372036854775708L)
        .endsWith(-7173733806442603340L)
        .hasSize(2049638230412172368L)
        .hasReplicas(host5, host2)
        .hasFraction(0.1111111111111111d, offset(.000000001d));
    assertThat(splits.get(2))
        .startsWith(-7173733806442603340L)
        .endsWith(-5124095576030430972L)
        .hasSize(2049638230412172368L)
        .hasReplicas(host5, host2)
        .hasFraction(0.1111111111111111d, offset(.000000001d));
    assertThat(splits.get(3))
        .startsWith(-5124095576030430972L)
        .endsWith(-3074457345618258603L)
        .hasSize(2049638230412172369L)
        .hasReplicas(host5, host2)
        .hasFraction(0.1111111111111111d, offset(.000000001d));

    // range M4 -> 3 splits
    assertThat(splits.get(5))
        .startsWith(-3074457345618258503L)
        .endsWith(-1024819115206086135L)
        .hasSize(2049638230412172368L)
        .hasReplicas(host6, host3)
        .hasFraction(0.1111111111111111d, offset(.000000001d));
    assertThat(splits.get(6))
        .startsWith(-1024819115206086135L)
        .endsWith(1024819115206086233L)
        .hasSize(2049638230412172368L)
        .hasReplicas(host6, host3)
        .hasFraction(0.1111111111111111d, offset(.000000001d));
    assertThat(splits.get(7))
        .startsWith(1024819115206086233L)
        .endsWith(3074457345618258602L)
        .hasSize(2049638230412172369L)
        .hasReplicas(host6, host3)
        .hasFraction(0.1111111111111111d, offset(.000000001d));

    // range M6 -> 3 splits
    assertThat(splits.get(9))
        .startsWith(3074457345618258702L)
        .endsWith(5124095576030431070L)
        .hasSize(2049638230412172368L)
        .hasReplicas(host1, host4)
        .hasFraction(0.1111111111111111d, offset(.000000001d));
    assertThat(splits.get(10))
        .startsWith(5124095576030431070L)
        .endsWith(7173733806442603438L)
        .hasSize(2049638230412172368L)
        .hasReplicas(host1, host4)
        .hasFraction(0.1111111111111111d, offset(.000000001d));
    assertThat(splits.get(11))
        .startsWith(7173733806442603438L)
        .endsWith(-9223372036854775808L)
        .hasSize(2049638230412172369L)
        .hasReplicas(host1, host4)
        .hasFraction(0.1111111111111111d, offset(.000000001d));

    // ranges M1, M3 and M5 stay intact because of their small size
    // which is also why we asked for 9 splits and ended up with 3 more.
    assertThat(splits.get(0))
        .startsWith(-9223372036854775808L)
        .endsWith(-9223372036854775708L)
        .hasSize(100L)
        .hasReplicas(host4, host2)
        .hasFraction(5.421010862427522E-18d, offset(.000000001d));
    assertThat(splits.get(4))
        .startsWith(-3074457345618258603L)
        .endsWith(-3074457345618258503L)
        .hasSize(100L)
        .hasReplicas(host5, host3)
        .hasFraction(5.421010862427522E-18d, offset(.000000001d));
    assertThat(splits.get(8))
        .startsWith(3074457345618258602L)
        .endsWith(3074457345618258702L)
        .hasSize(100L)
        .hasReplicas(host1, host6)
        .hasFraction(5.421010862427522E-18d, offset(.000000001d));

    assertThat(splits.stream().map(TokenRange::fraction).reduce(0d, (f1, f2) -> f1 + f2))
        .isEqualTo(1d, offset(.000000001));
  }

  private com.datastax.driver.core.TokenRange range(long start, long end) {
    return newTokenRange(newToken(start), newToken(end));
  }
}
