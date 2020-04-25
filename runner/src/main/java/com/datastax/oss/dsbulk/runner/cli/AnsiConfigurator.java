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
package com.datastax.oss.dsbulk.runner.cli;

import java.util.Arrays;
import java.util.Iterator;
import org.fusesource.jansi.AnsiConsole;

public class AnsiConfigurator {

  public static void configureAnsi(String... args) throws ParseException {
    // log.ansiMode should logically be handled by LogSettings,
    // but this setting has to be processed very early
    // (before the console is used).
    String ansiMode = "normal";
    Iterator<String> iterator = Arrays.asList(args).iterator();
    while (iterator.hasNext()) {
      String arg = iterator.next();
      if (arg.equals("--log.ansiMode")) {
        if (iterator.hasNext()) {
          ansiMode = iterator.next();
          break;
        } else {
          throw new ParseException("Expecting value after --log.ansiMode");
        }
      }
    }
    if (ansiMode.equals("disabled")) {
      System.setProperty("jansi.strip", "true");
    } else if (ansiMode.equals("force")) {
      System.setProperty("jansi.force", "true");
    }
    AnsiConsole.systemInstall();
  }
}
