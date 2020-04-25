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

import com.datastax.oss.dsbulk.connectors.api.Field;

/**
 * A field in a mapping definition. Fields can be {@linkplain IndexedMappingField indexed} or
 * {@linkplain MappedMappingField named} (mapped). In a mapping definition, they appear on the left
 * side of a mapping entry.
 */
public interface MappingField extends MappingToken, Field {}
