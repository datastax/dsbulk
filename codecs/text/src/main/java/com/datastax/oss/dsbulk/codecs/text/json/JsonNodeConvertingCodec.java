/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.codecs.text.json;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.dsbulk.codecs.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

public abstract class JsonNodeConvertingCodec<T> extends ConvertingCodec<JsonNode, T> {

  private final List<String> nullStrings;

  protected JsonNodeConvertingCodec(TypeCodec<T> targetCodec, List<String> nullStrings) {
    super(targetCodec, JsonNode.class);
    this.nullStrings = nullStrings;
  }

  /**
   * Whether the input is null.
   *
   * <p>This method should be used to inspect external inputs that are meant to be converted <em>to
   * textual CQL types only (text, varchar and ascii)</em>.
   *
   * <p>It always considers the empty string as NOT equivalent to NULL, unless the user clearly
   * specifies that the empty string is to be considered as NULL, through the <code>
   * codec.nullStrings</code> setting.
   *
   * <p>Do NOT use this method for non-textual CQL types; use {@link #isNullOrEmpty(JsonNode)}
   * instead.
   */
  protected boolean isNull(JsonNode node) {
    return node == null
        || node.isNull()
        || node.isMissingNode()
        || (node.isValueNode() && nullStrings.contains(node.asText()));
  }

  /**
   * Whether the input is null or empty.
   *
   * <p>This method should be used to inspect external inputs that are meant to be converted <em>to
   * non-textual CQL types only</em>.
   *
   * <p>It always considers the empty string as equivalent to NULL, which is in compliance with the
   * documentation of <code>codec.nullStrings</code>: "Note that, regardless of this setting, DSBulk
   * will always convert empty strings to `null` if the target CQL type is not textual (i.e. not
   * text, varchar or ascii)."
   *
   * <p>Do NOT use this method for textual CQL types; use {@link #isNull(JsonNode)} instead.
   */
  protected boolean isNullOrEmpty(JsonNode node) {
    return isNull(node) || (node.isValueNode() && node.asText().isEmpty());
  }
}
