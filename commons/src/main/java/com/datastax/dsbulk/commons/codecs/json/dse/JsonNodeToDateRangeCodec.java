/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.json.dse;

import static com.datastax.dsbulk.commons.codecs.json.JsonCodecUtils.JSON_NODE_FACTORY;

import com.datastax.dsbulk.commons.codecs.json.JsonNodeConvertingCodec;
import com.datastax.dsbulk.commons.codecs.util.CodecUtils;
import com.datastax.dse.driver.api.core.data.time.DateRange;
import com.datastax.dse.driver.internal.core.type.codec.time.DateRangeCodec;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

public class JsonNodeToDateRangeCodec extends JsonNodeConvertingCodec<DateRange> {

  public JsonNodeToDateRangeCodec(List<String> nullStrings) {
    super(new DateRangeCodec(), nullStrings);
  }

  @Override
  public DateRange externalToInternal(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    return CodecUtils.parseDateRange(node.asText());
  }

  @Override
  public JsonNode internalToExternal(DateRange value) {
    return value == null ? null : JSON_NODE_FACTORY.textNode(value.toString());
  }
}
