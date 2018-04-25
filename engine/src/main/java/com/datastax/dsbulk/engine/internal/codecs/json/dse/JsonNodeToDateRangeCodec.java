/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json.dse;

import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.JSON_NODE_FACTORY;

import com.datastax.driver.dse.search.DateRange;
import com.datastax.driver.dse.search.DateRangeCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

public class JsonNodeToDateRangeCodec extends JsonNodeConvertingCodec<DateRange> {

  public JsonNodeToDateRangeCodec(List<String> nullStrings) {
    super(DateRangeCodec.INSTANCE, nullStrings);
  }

  @Override
  public DateRange externalToInternal(JsonNode node) {
    if (isNull(node)) {
      return null;
    }
    return CodecUtils.parseDateRange(node.asText());
  }

  @Override
  public JsonNode internalToExternal(DateRange value) {
    if (value == null) {
      return JSON_NODE_FACTORY.nullNode();
    }
    return JSON_NODE_FACTORY.textNode(value.toString());
  }
}
