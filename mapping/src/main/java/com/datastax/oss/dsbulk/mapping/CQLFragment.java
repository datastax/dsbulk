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

/**
 * A fragment of CQL language included in a mapping definition or in a CQL query. Fragments can be
 * {@linkplain CQLWord CQL identifiers}, {@linkplain FunctionCall function calls}, or {@linkplain
 * CQLLiteral CQL literals}. In a mapping definition, they appear on the right side of a mapping
 * entry.
 */
public interface CQLFragment extends MappingToken {

  String render(CQLRenderMode mode);
}
