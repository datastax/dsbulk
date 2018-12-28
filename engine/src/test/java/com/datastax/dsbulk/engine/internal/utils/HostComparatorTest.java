/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Host;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

class HostComparatorTest {

  @ParameterizedTest(name = "Compare on address1: {0} and address1: {1} should return: {2}")
  @ArgumentsSource(Hosts.class)
  void should_compare_two_hosts(InetSocketAddress a1, InetSocketAddress a2, int expectedResult) {
    // given
    Host host1 = createHostWithAddressAndPort(a1);
    Host host2 = createHostWithAddressAndPort(a2);

    // when
    int compare = new HostComparator().compare(host1, host2);

    // then
    assertThat(compare).isEqualTo(expectedResult);
  }

  private static class Hosts implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      InetAddress nullInetAddress = null;
      List<Arguments> args = new ArrayList<>();
      args.add(
          Arguments.of(
              InetSocketAddress.createUnresolved("1.2.3.4", 9090),
              InetSocketAddress.createUnresolved("1.2.3.5", 9090),
              -1));
      args.add(
          Arguments.of(
              new InetSocketAddress(nullInetAddress, 9090),
              new InetSocketAddress(nullInetAddress, 9090),
              0));
      args.add(Arguments.of(null, null, 0));
      args.add(Arguments.of(null, InetSocketAddress.createUnresolved("1.2.3.5", 9090), 0));
      return args.stream();
    }
  }

  private Host createHostWithAddressAndPort(InetSocketAddress address) {
    Host h1 = mock(Host.class);
    when(h1.getSocketAddress()).thenReturn(address);
    return h1;
  }
}
