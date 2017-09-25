/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.io;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dsbulk.commons.url.LoaderURLStreamHandlerFactory;
import com.google.common.base.Charsets;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.Test;

public class FileSplitterTest {

  static {
    try {
      URL.setURLStreamHandlerFactory(new LoaderURLStreamHandlerFactory());
    } catch (Throwable ignore) {
    }
  }

  @Test
  public void should_map_entire_file_in_one_chunk_without_header() throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f', '\n'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, false, 0, 8);
    List<URL> chunks = splitter.split();
    assertThat(chunks).hasSize(1);
    assertThat(chunks.get(0).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=0-8", file));
  }

  @Test
  public void should_map_entire_file_in_one_chunk_with_header() throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f', '\n'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, true, 0, 8);
    List<URL> chunks = splitter.split();
    assertThat(chunks).hasSize(1);
    assertThat(chunks.get(0).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=0-4&range=4-8", file));
  }

  @Test
  public void should_map_entire_file_in_one_chunk_without_header_skipping_one_line()
      throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f', '\n'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, false, 1, 8);
    List<URL> chunks = splitter.split();
    assertThat(chunks).hasSize(1);
    assertThat(chunks.get(0).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=4-8", file));
  }

  @Test
  public void should_map_entire_file_in_one_chunk_with_header_skipping_one_line() throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f', '\n'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, true, 1, 8);
    List<URL> chunks = splitter.split();
    assertThat(chunks).isEmpty();
  }

  @Test
  public void should_map_entire_file_in_one_chunk_with_header_skipping_one_line_bis()
      throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f', '\n', 'g', 'h', 'i', '\n'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, true, 1, 8);
    List<URL> chunks = splitter.split();
    assertThat(chunks).hasSize(1);
    assertThat(chunks.get(0).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=0-4&range=8-12", file));
  }

  @Test
  public void should_map_entire_file_in_one_chunk_without_header_skipping_two_lines()
      throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f', '\n'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, false, 2, 8);
    List<URL> chunks = splitter.split();
    assertThat(chunks).isEmpty();
  }

  @Test
  public void should_map_entire_file_in_one_chunk_without_header_skipping_three_lines()
      throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f', '\n'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, false, 3, 8);
    List<URL> chunks = splitter.split();
    assertThat(chunks).isEmpty();
  }

  @Test
  public void should_map_entire_file_in_one_chunk_without_header_2_threads() throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f', '\n'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, false, 0, 4);
    List<URL> chunks = splitter.split();
    assertThat(chunks).hasSize(2);
    assertThat(chunks.get(0).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=0-4", file));
    assertThat(chunks.get(1).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=4-8", file));
  }

  @Test
  public void should_map_entire_file_in_one_chunk_without_header_4_threads() throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f', '\n'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, false, 0, 2);
    List<URL> chunks = splitter.split();
    assertThat(chunks).hasSize(2);
    assertThat(chunks.get(0).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=0-4", file));
    assertThat(chunks.get(1).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=4-8", file));
  }

  @Test
  public void should_map_entire_file_in_one_chunk_with_header_4_threads() throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f', '\n'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, true, 0, 2);
    List<URL> chunks = splitter.split();
    assertThat(chunks).hasSize(1);
    assertThat(chunks.get(0).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=0-4&range=4-8", file));
  }

  @Test
  public void should_map_entire_file_in_one_chunk_with_header_4_threads_skipping_one_line()
      throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f', '\n'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, true, 1, 2);
    List<URL> chunks = splitter.split();
    assertThat(chunks).isEmpty();
  }

  @Test
  public void should_map_entire_file_in_one_chunk_without_header_no_eof() throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, false, 0, 8);
    List<URL> chunks = splitter.split();
    assertThat(chunks).hasSize(1);
    assertThat(chunks.get(0).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=0-7", file));
  }

  @Test
  public void should_map_entire_file_in_one_chunk_with_header_no_eof() throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, true, 0, 8);
    List<URL> chunks = splitter.split();
    assertThat(chunks).hasSize(1);
    assertThat(chunks.get(0).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=0-4&range=4-7", file));
  }

  @Test
  public void should_map_entire_file_in_one_chunk_without_header_skipping_one_line_no_eof()
      throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, false, 1, 8);
    List<URL> chunks = splitter.split();
    assertThat(chunks).hasSize(1);
    assertThat(chunks.get(0).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=4-7", file));
  }

  @Test
  public void should_map_entire_file_in_one_chunk_with_header_skipping_one_line_no_eof()
      throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, true, 1, 8);
    List<URL> chunks = splitter.split();
    assertThat(chunks).isEmpty();
  }

  @Test
  public void should_map_entire_file_in_one_chunk_with_header_skipping_one_line_bis_no_eof()
      throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f', '\n', 'g', 'h', 'i'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, true, 1, 8);
    List<URL> chunks = splitter.split();
    assertThat(chunks).hasSize(1);
    assertThat(chunks.get(0).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=0-4&range=8-11", file));
  }

  @Test
  public void should_map_entire_file_in_one_chunk_without_header_skipping_two_lines_no_eof()
      throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, false, 2, 8);
    List<URL> chunks = splitter.split();
    assertThat(chunks).isEmpty();
  }

  @Test
  public void should_map_entire_file_in_one_chunk_without_header_skipping_three_lines_no_eof()
      throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, false, 3, 8);
    List<URL> chunks = splitter.split();
    assertThat(chunks).isEmpty();
  }

  @Test
  public void should_map_entire_file_in_one_chunk_without_header_2_threads_no_eof()
      throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, false, 0, 4);
    List<URL> chunks = splitter.split();
    assertThat(chunks).hasSize(2);
    assertThat(chunks.get(0).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=0-4", file));
    assertThat(chunks.get(1).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=4-7", file));
  }

  @Test
  public void should_map_entire_file_in_one_chunk_without_header_4_threads_no_eof()
      throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, false, 0, 2);
    List<URL> chunks = splitter.split();
    assertThat(chunks).hasSize(2);
    assertThat(chunks.get(0).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=0-4", file));
    assertThat(chunks.get(1).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=4-7", file));
  }

  @Test
  public void should_map_entire_file_in_one_chunk_with_header_4_threads_no_eof() throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, true, 0, 2);
    List<URL> chunks = splitter.split();
    assertThat(chunks).hasSize(1);
    assertThat(chunks.get(0).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=0-4&range=4-7", file));
  }

  @Test
  public void should_map_entire_file_in_one_chunk_with_header_4_threads_skipping_one_line_no_eof()
      throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, true, 1, 2);
    List<URL> chunks = splitter.split();
    assertThat(chunks).isEmpty();
  }

  ///
  @Test
  public void should_map_entire_file_in_one_chunk_without_header_odd_memory_size()
      throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f', '\n'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, false, 0, 5);
    List<URL> chunks = splitter.split();
    assertThat(chunks).hasSize(2);
    assertThat(chunks.get(0).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=0-4", file));
    assertThat(chunks.get(1).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=4-8", file));
  }

  @Test
  public void should_map_entire_file_in_one_chunk_with_header_odd_memory_size() throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f', '\n'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, true, 0, 5);
    List<URL> chunks = splitter.split();
    assertThat(chunks).hasSize(1);
    assertThat(chunks.get(0).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=0-4&range=4-8", file));
  }

  @Test
  public void should_map_entire_file_in_one_chunk_without_header_skipping_one_line_odd_memory_size()
      throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f', '\n'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, false, 1, 5);
    List<URL> chunks = splitter.split();
    assertThat(chunks).hasSize(1);
    assertThat(chunks.get(0).toExternalForm())
        .isEqualTo(String.format("mapped-file:%s?range=4-8", file));
  }

  @Test
  public void should_map_entire_file_in_one_chunk_with_header_skipping_one_line_odd_memory_size()
      throws Exception {
    Path file = Files.createTempFile("test", ".txt");
    Files.write(file, new byte[] {'a', 'b', 'c', '\n', 'd', 'e', 'f', '\n'});
    FileSplitter splitter = new FileSplitter(file.toFile(), Charsets.US_ASCII, true, 1, 5);
    List<URL> chunks = splitter.split();
    assertThat(chunks).isEmpty();
  }
}
