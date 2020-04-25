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

import com.datastax.oss.dsbulk.connectors.api.DefaultMappedField;
import edu.umd.cs.findbugs.annotations.NonNull;

/** A field in a mapping definition identified by an alphanumeric name. */
public class MappedMappingField extends DefaultMappedField implements MappingField {

  public MappedMappingField(@NonNull String name) {
    super(name);
  }
}
