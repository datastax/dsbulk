/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.codecs.text.string;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import java.net.InetAddress;
import java.util.List;

public class StringToInetAddressCodec extends StringConvertingCodec<InetAddress> {

  public StringToInetAddressCodec(List<String> nullStrings) {
    super(TypeCodecs.INET, nullStrings);
  }

  @Override
  public InetAddress externalToInternal(String s) {
    if (isNullOrEmpty(s)) {
      return null;
    }
    if (s.isEmpty()) {
      throw new IllegalArgumentException("Cannot create inet address from empty string");
    }
    try {
      return InetAddress.getByName(s);
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot parse inet address: " + s);
    }
  }

  @Override
  public String internalToExternal(InetAddress value) {
    if (value == null) {
      return nullString();
    }
    return value.getHostAddress();
  }
}
