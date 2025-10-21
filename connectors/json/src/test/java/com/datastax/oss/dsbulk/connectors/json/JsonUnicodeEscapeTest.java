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
package com.datastax.oss.dsbulk.connectors.json;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.dsbulk.codecs.text.json.JsonNodeToStringCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * Test for bug issue #508: JSON connector should preserve unicode escape sequences.
 */
class JsonUnicodeEscapeTest {

  @Test
  void should_preserve_unicode_escape_sequences_in_text_nodes() {
    // Create a string with unicode escape sequences
    String testString = "\\u001a\\u001aL\\\\"; // The problematic string from the issue #508

    // Create a JSON node with the test string
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode node = new TextNode(testString);

    // Create the codec that would handle the string conversion
    JsonNodeToStringCodec codec = new JsonNodeToStringCodec(
        null, // inner codec not used for this test
        objectMapper,
        Collections.emptyList() // null strings
    );

    // Convert the node to a string
    String result = codec.externalToInternal(node);

    // Check if the conversion preserves the original string with escape sequences
    assertThat(result).isEqualTo(testString);
  }

  @Test
  void should_handle_regular_text_properly() {
    // Regular text without escape sequences
    String testString = "This is a regular string";

    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode node = new TextNode(testString);

    JsonNodeToStringCodec codec = new JsonNodeToStringCodec(
        null,
        objectMapper,
        Collections.emptyList()
    );

    String result = codec.externalToInternal(node);
    assertThat(result).isEqualTo(testString);
  }

  @Test
  void should_handle_mixed_content_properly() {
    // Text with both regular characters and unicode escape sequences
    String testString = "Text with \\u001a unicode \\u001aL\\\\ escapes";

    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode node = new TextNode(testString);

    JsonNodeToStringCodec codec = new JsonNodeToStringCodec(
        null,
        objectMapper,
        Collections.emptyList()
    );

    String result = codec.externalToInternal(node);
    assertThat(result).isEqualTo(testString);
  }
}