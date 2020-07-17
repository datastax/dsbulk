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
package com.datastax.oss.dsbulk.workflow.commons.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class AddressUtilsTest {

  @ParameterizedTest
  @CsvSource({
    // IPv4 without port
    "192.168.0.1,9043,192.168.0.1:9043",
    // Unambiguous IPv6 without port (because Ipv6 literal is in full form)
    "fe80:0:0:0:f861:3eff:fe1d:9d7b,9043,fe80:0:0:0:f861:3eff:fe1d:9d7b:9043",
    "fe80:0:0:f861:3eff:fe1d:9d7b:9044,9043,fe80:0:0:f861:3eff:fe1d:9d7b:9044:9043",
    // Unambiguous IPv6 without port (because last block is not numeric)
    "fe80::f861:3eff:fe1d:9d7b,9043,fe80::f861:3eff:fe1d:9d7b:9043",
    // Ambiguous IPv6 without port
    "fe80::f861:3eff:fe1d:1234,9043,fe80::f861:3eff:fe1d:1234",
    // hostname without port
    "host.com,9043,host.com:9043",
    // IPv4 with port
    "192.168.0.1:9044,9043,192.168.0.1:9044",
    // Unambiguous IPv6 with port (because Ipv6 literal is in full form)
    "fe80:0:0:0:f861:3eff:fe1d:9d7b:9044,9043,fe80:0:0:0:f861:3eff:fe1d:9d7b:9044",
    "fe80:0:0:0:f861:3eff:fe1d:1234:9044,9043,fe80:0:0:0:f861:3eff:fe1d:1234:9044",
    // Unambiguous IPv6 with port (because last block is not numeric)
    "fe80::f861:3eff:fe1d:9d7b:9044,9043,fe80::f861:3eff:fe1d:9d7b:9044",
    // Ambiguous IPv6 with port
    "fe80::f861:3eff:fe1d:1234:9044,9043,fe80::f861:3eff:fe1d:1234:9044",
    // hostname with port
    "host.com:9044,9043,host.com:9044",
  })
  void should_produce_expected_contact_point(String input, int defaultPort, String expected) {
    String actual = AddressUtils.maybeAddPortToHost(input, defaultPort);
    assertThat(actual).isEqualTo(expected);
  }
}
