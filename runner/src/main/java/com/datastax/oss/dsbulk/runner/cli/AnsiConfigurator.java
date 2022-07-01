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

import java.util.Iterator;
import java.util.List;
import org.fusesource.jansi.AnsiConsole;

public class AnsiConfigurator {

  public enum AnsiMode {
    normal,
    disabled,
    force
  }

  public static void configureAnsi(List<String> args) throws ParseException {
    // log.ansiMode should logically be handled by LogSettings,
    // but this setting has to be processed very early
    // (before the console is used).
    AnsiMode ansiMode = AnsiMode.normal;
    Iterator<String> iterator = args.iterator();
    while (iterator.hasNext()) {
      String arg = iterator.next();
      if (arg.equals("--log.ansiMode") || arg.equals("--dsbulk.log.ansiMode")) {
        if (iterator.hasNext()) {
          ansiMode = parseAnsiMode(arg, iterator.next());
          break;
        } else {
          throw new ParseException("Expecting value after: " + arg);
        }
      } else if (arg.startsWith("--log.ansiMode=") || arg.startsWith("--dsbulk.log.ansiMode=")) {
        int i = arg.indexOf('=');
        ansiMode = parseAnsiMode(arg.substring(0, i), arg.substring(i + 1));
      }
    }
    if (ansiMode == AnsiMode.disabled) {
      System.setProperty("jansi.strip", "true");
    } else if (ansiMode == AnsiMode.force) {
      System.setProperty("jansi.force", "true");
    }
    AnsiConsole.systemInstall();
  }

  private static AnsiMode parseAnsiMode(String name, String value) throws ParseException {
    try {
      return AnsiMode.valueOf(value);
    } catch (IllegalArgumentException e) {
      throw new ParseException(
          String.format(
              "Invalid value for %s, expecting one of '%s', '%s', '%s', got: '%s'",
              name, AnsiMode.normal, AnsiMode.disabled, AnsiMode.force, value));
    }
  }
}
