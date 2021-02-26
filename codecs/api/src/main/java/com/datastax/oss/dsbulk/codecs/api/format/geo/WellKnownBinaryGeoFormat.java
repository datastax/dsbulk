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
package com.datastax.oss.dsbulk.codecs.api.format.geo;

import com.datastax.dse.driver.api.core.data.geometry.Geometry;
import com.datastax.oss.dsbulk.codecs.api.format.binary.Base64BinaryFormat;
import com.datastax.oss.dsbulk.codecs.api.format.binary.BinaryFormat;
import com.datastax.oss.dsbulk.codecs.api.format.binary.HexBinaryFormat;
import edu.umd.cs.findbugs.annotations.Nullable;

public class WellKnownBinaryGeoFormat implements GeoFormat {

  public static final WellKnownBinaryGeoFormat HEX_INSTANCE =
      new WellKnownBinaryGeoFormat(HexBinaryFormat.INSTANCE);

  public static final WellKnownBinaryGeoFormat BASE64_INSTANCE =
      new WellKnownBinaryGeoFormat(Base64BinaryFormat.INSTANCE);

  private final BinaryFormat binaryFormat;

  private WellKnownBinaryGeoFormat(BinaryFormat binaryFormat) {
    this.binaryFormat = binaryFormat;
  }

  @Nullable
  @Override
  public String format(@Nullable Geometry geo) {
    return geo == null ? null : binaryFormat.format(geo.asWellKnownBinary());
  }
}
