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
package com.datastax.oss.dsbulk.codecs.jdk;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.SetType;
import edu.umd.cs.findbugs.annotations.NonNull;

public class JdkCodecUtils {

  public static boolean isNumeric(@NonNull DataType cqlType) {
    return cqlType == DataTypes.TINYINT
        || cqlType == DataTypes.SMALLINT
        || cqlType == DataTypes.INT
        || cqlType == DataTypes.BIGINT
        || cqlType == DataTypes.FLOAT
        || cqlType == DataTypes.DOUBLE
        || cqlType == DataTypes.VARINT
        || cqlType == DataTypes.DECIMAL;
  }

  public static boolean isCollection(@NonNull DataType cqlType) {
    return cqlType instanceof SetType || cqlType instanceof ListType;
  }

  public static boolean isTemporal(@NonNull DataType cqlType) {
    return cqlType == DataTypes.DATE || cqlType == DataTypes.TIME || cqlType == DataTypes.TIMESTAMP;
  }

  public static boolean isUUID(@NonNull DataType cqlType) {
    return cqlType == DataTypes.UUID || cqlType == DataTypes.TIMEUUID;
  }
}
