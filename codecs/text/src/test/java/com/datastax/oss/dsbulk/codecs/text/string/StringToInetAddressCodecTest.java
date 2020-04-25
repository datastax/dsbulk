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
package com.datastax.oss.dsbulk.codecs.text.string;

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.net.InetAddress;
import org.junit.jupiter.api.Test;

class StringToInetAddressCodecTest {

  private StringToInetAddressCodec codec = new StringToInetAddressCodec(Lists.newArrayList("NULL"));

  @Test
  void should_convert_from_valid_external() throws Exception {
    assertThat(codec)
        .convertsFromExternal("1.2.3.4")
        .toInternal(InetAddress.getByName("1.2.3.4"))
        .convertsFromExternal("127.0.0.1")
        .toInternal(InetAddress.getByName("127.0.0.1"))
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() throws Exception {
    assertThat(codec)
        .convertsFromInternal(InetAddress.getByName("1.2.3.4"))
        .toExternal("1.2.3.4")
        .convertsFromInternal(InetAddress.getByName("127.0.0.1"))
        .toExternal("127.0.0.1")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec).cannotConvertFromExternal("not a valid inet address");
  }
}
