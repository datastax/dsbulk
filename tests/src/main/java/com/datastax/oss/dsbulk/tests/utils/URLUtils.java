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
package com.datastax.oss.dsbulk.tests.utils;

import com.datastax.oss.dsbulk.commons.url.BulkLoaderURLStreamHandlerFactory;
import java.net.URL;

public class URLUtils {

  public static void setURLFactoryIfNeeded() {
    try {
      URL.setURLStreamHandlerFactory(new BulkLoaderURLStreamHandlerFactory());
    } catch (Throwable t) {
      // URL.setURLStreamHandlerFactory throws an Error if it's been set more then once
      // Ignore that and keep going.
    }
  }
}
