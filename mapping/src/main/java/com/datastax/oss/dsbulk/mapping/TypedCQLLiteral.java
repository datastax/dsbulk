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
package com.datastax.oss.dsbulk.mapping;

import com.datastax.oss.driver.api.core.type.DataType;
import edu.umd.cs.findbugs.annotations.NonNull;

public class TypedCQLLiteral extends CQLLiteral implements MappingField {

  private final DataType dataType;

  public TypedCQLLiteral(@NonNull String literal, @NonNull DataType dataType) {
    super(literal);
    this.dataType = dataType;
  }

  @NonNull
  public DataType getDataType() {
    return dataType;
  }

  @Override
  public String render(CQLRenderMode mode) {
    String castType = dataType.asCql(false, false);
    switch (mode) {
      case ALIASED_SELECTOR:
        return String.format(
            "(%s)%s AS \"(%s)%s\"", castType, this.getLiteral(), castType, this.getLiteral());
      case UNALIASED_SELECTOR:
      case INTERNAL:
        return String.format("(%s)%s", castType, this.getLiteral());
      default:
        return super.render(mode);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TypedCQLLiteral)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    TypedCQLLiteral that = (TypedCQLLiteral) o;
    return dataType.equals(that.dataType);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + dataType.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return getFieldDescription();
  }

  @NonNull
  @Override
  public String getFieldDescription() {
    return render(CQLRenderMode.INTERNAL);
  }
}
