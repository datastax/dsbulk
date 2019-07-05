/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.config;

import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class URLsFromFileLoader {
  /**
   * Loads list of URLs from a file given as the urlfile argument using encoding.
   *
   * @param urlfile - path to file passed as the --urlfile argument to dsbulk
   * @param encoding - encoding to use when reading the list of URLs from given file
   * @return list of urls resolved from urlfile line by line
   * @throws IOException if unable to load a file from urlfile path
   */
  public static List<URL> getURLs(Path urlfile, Charset encoding) throws IOException {
    List<URL> result = new ArrayList<>();
    List<String> paths = Files.readAllLines(urlfile, encoding);
    for (String path : paths) {
      try {
        if (!path.startsWith("#")) {
          result.add(ConfigUtils.resolveURL(path.trim()));
        }
      } catch (Exception e) {
        throw new BulkConfigurationException(
            String.format("%s: Expecting valid filepath or URL, got '%s'", urlfile, path), e);
      }
    }
    return result;
  }
}
