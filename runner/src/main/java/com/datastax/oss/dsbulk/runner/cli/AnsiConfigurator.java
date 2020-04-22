/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
