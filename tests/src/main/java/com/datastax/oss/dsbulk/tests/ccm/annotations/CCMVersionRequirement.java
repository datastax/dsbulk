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
package com.datastax.oss.dsbulk.tests.ccm.annotations;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import com.datastax.oss.dsbulk.tests.ccm.CCMCluster.Type;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface CCMVersionRequirement {

  /** @return the cluster type this requirement refers to (OSS, DDAC or DSE). */
  Type type();

  /** @return The minimum version required to execute this test (inclusive), i.e. "4.8.14" */
  String min() default "";

  /**
   * @return the maximum exclusive version allowed to execute this test, i.e. "5.1.2" means only
   *     tests &lt; "5.1.2" may execute this test.
   */
  String max() default "";
}
