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
package com.datastax.oss.dsbulk.partitioner;

import static com.datastax.oss.dsbulk.partitioner.assertions.PartitionerAssertions.assertThat;
import static com.datastax.oss.dsbulk.tests.driver.DriverUtils.newToken;
import static com.datastax.oss.dsbulk.tests.driver.DriverUtils.newTokenRange;
import static java.net.InetSocketAddress.createUnresolved;
import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.offset;
import static org.assertj.core.util.Sets.newLinkedHashSet;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.dsbulk.partitioner.murmur3.Murmur3BulkTokenFactory;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class PartitionGeneratorTest {

  private final Murmur3BulkTokenFactory tokenFactory = new Murmur3BulkTokenFactory();

  // A typical ring with Murmur3 and 3 nodes, single DC, SimpleStrategy:

  private final TokenRange rangeS1 = range(-9223372036854775808L, -3074457345618258603L);

  private final TokenRange rangeS2 = range(-3074457345618258603L, 3074457345618258602L);

  private final TokenRange rangeS3 = range(3074457345618258602L, -9223372036854775808L);

  private final Set<TokenRange> singleDCRanges = newLinkedHashSet(rangeS1, rangeS2, rangeS3);

  // A typical ring with Murmur3 and 6 nodes, 2 DCs, NetworkTopologyStrategy:

  private final TokenRange rangeM1 = range(-9223372036854775808L, -9223372036854775708L); // 100

  private final TokenRange rangeM2 = range(-9223372036854775708L, -3074457345618258603L);

  private final TokenRange rangeM3 = range(-3074457345618258603L, -3074457345618258503L); // 100

  private final TokenRange rangeM4 = range(-3074457345618258503L, 3074457345618258602L);

  private final TokenRange rangeM5 = range(3074457345618258602L, 3074457345618258702L); // 100

  private final TokenRange rangeM6 = range(3074457345618258702L, -9223372036854775808L);

  private final Set<TokenRange> multiDCRanges =
      newLinkedHashSet(rangeM1, rangeM2, rangeM3, rangeM4, rangeM5, rangeM6);

  @Mock private KeyspaceMetadata keyspace;

  @Mock private TokenMap tokenMap;

  @Mock private Node host1;
  @Mock private Node host2;
  @Mock private Node host3;
  @Mock private Node host4;
  @Mock private Node host5;
  @Mock private Node host6;

  @BeforeEach
  void setUp() {

    MockitoAnnotations.initMocks(this);

    CqlIdentifier ks = CqlIdentifier.fromInternal("ks");
    when(keyspace.getName()).thenReturn(ks);

    when(tokenMap.getReplicas(ks, rangeS1)).thenReturn(singleton(host1));
    when(tokenMap.getReplicas(ks, rangeS2)).thenReturn(singleton(host2));
    when(tokenMap.getReplicas(ks, rangeS3)).thenReturn(singleton(host3));

    when(tokenMap.getReplicas(ks, rangeM1)).thenReturn(newLinkedHashSet(host2, host4));
    when(tokenMap.getReplicas(ks, rangeM2)).thenReturn(newLinkedHashSet(host2, host5));
    when(tokenMap.getReplicas(ks, rangeM3)).thenReturn(newLinkedHashSet(host3, host5));
    when(tokenMap.getReplicas(ks, rangeM4)).thenReturn(newLinkedHashSet(host3, host6));
    when(tokenMap.getReplicas(ks, rangeM5)).thenReturn(newLinkedHashSet(host1, host6));
    when(tokenMap.getReplicas(ks, rangeM6)).thenReturn(newLinkedHashSet(host1, host4));

    when(host1.getEndPoint())
        .thenReturn(new DefaultEndPoint(createUnresolved("192.168.1.1", 9042)));
    when(host2.getEndPoint())
        .thenReturn(new DefaultEndPoint(createUnresolved("192.168.1.2", 9042)));
    when(host3.getEndPoint())
        .thenReturn(new DefaultEndPoint(createUnresolved("192.168.1.3", 9042)));
    when(host4.getEndPoint())
        .thenReturn(new DefaultEndPoint(createUnresolved("192.168.1.4", 9042)));
    when(host5.getEndPoint())
        .thenReturn(new DefaultEndPoint(createUnresolved("192.168.1.5", 9042)));
    when(host6.getEndPoint())
        .thenReturn(new DefaultEndPoint(createUnresolved("192.168.1.6", 9042)));
  }

  @Test
  void should_split_single_dc() {

    given(tokenMap.getTokenRanges()).willReturn(singleDCRanges);

    PartitionGenerator generator =
        new PartitionGenerator(keyspace.getName(), tokenMap, tokenFactory);
    List<BulkTokenRange> splits = generator.partition(9);

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

    assertThat(splits.stream().map(BulkTokenRange::fraction).reduce(0d, Double::sum))
        .isEqualTo(1d, offset(.000000001));
  }

  @Test
  void should_split_multi_dc() {

    given(tokenMap.getTokenRanges()).willReturn(multiDCRanges);

    PartitionGenerator generator =
        new PartitionGenerator(keyspace.getName(), tokenMap, tokenFactory);
    List<BulkTokenRange> splits = generator.partition(9);

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

    assertThat(splits.stream().map(BulkTokenRange::fraction).reduce(0d, Double::sum))
        .isEqualTo(1d, offset(.000000001));
  }

  private TokenRange range(long start, long end) {
    return newTokenRange(newToken(start), newToken(end));
  }
}
