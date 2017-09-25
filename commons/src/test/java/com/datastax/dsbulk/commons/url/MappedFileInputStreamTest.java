/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.url;

import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

public class MappedFileInputStreamTest {

  static {
    try {
      URL.setURLStreamHandlerFactory(new LoaderURLStreamHandlerFactory());
    } catch (Throwable ignore) {
    }
  }

  @Test
  public void should_map_in_one_single_buffer() throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, newArrayList("line1", "line2"));
    MappedFileInputStream is = new MappedFileInputStream(new URL("mapped-file:" + file));
    List<String> lines =
        new BufferedReader(new InputStreamReader(is)).lines().collect(Collectors.toList());
    assertThat(lines).containsExactly("line1", "line2");
  }

  @Test
  public void should_map_in_two_buffers() throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, newArrayList("line1", "line2"));
    long length = Files.size(file);
    MappedFileInputStream is =
        new MappedFileInputStream(
            new URL(
                String.format(
                    "mapped-file:" + file + "?range=0-%d&range=%d-%d",
                    length / 2,
                    length / 2,
                    length)));
    List<String> lines =
        new BufferedReader(new InputStreamReader(is)).lines().collect(Collectors.toList());
    assertThat(lines).containsExactly("line1", "line2");
  }
}
