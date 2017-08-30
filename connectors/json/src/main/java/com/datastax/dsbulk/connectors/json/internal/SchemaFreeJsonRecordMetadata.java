/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.json.internal;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeTokens;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.google.common.reflect.TypeToken;
import java.math.BigDecimal;
import java.math.BigInteger;

public class SchemaFreeJsonRecordMetadata implements RecordMetadata {

  /**
   * {@inheritDoc}
   *
   * <p>This implementation assumes no schema (i.e., all fields are assumed to exist), and returns
   * the most appropriate type that Json is capable of handling natively:
   *
   * <ol>
   *   <li>CQL booleans are mapped to Json booleans;
   *   <li>CQL numeric types are mapped to Json numbers;
   *   <li>CQL sets and lists are mapped to Json arrays;
   *   <li>CQL maps are mapped to Json objects;
   *   <li>All other CQL types, and in particular temporal types, are mapped to Json strings.
   * </ol>
   *
   * @param field the field name.
   * @param cqlType the CQL type associated with the given field.
   * @return the type of the given field, according to the rules outlined above.
   */
  @Override
  public TypeToken<?> getFieldType(String field, DataType cqlType) {
    switch (cqlType.getName()) {

        // booleans translate to Json booleans
      case BOOLEAN:
        return TypeToken.of(Boolean.class);

        // numeric types: translate to Json numbers
      case TINYINT:
        return TypeToken.of(Byte.class);
      case SMALLINT:
        return TypeToken.of(Short.class);
      case INT:
        return TypeToken.of(Integer.class);
      case BIGINT:
        return TypeToken.of(Long.class);
      case VARINT:
        return TypeToken.of(BigInteger.class);
      case FLOAT:
        return TypeToken.of(Float.class);
      case DOUBLE:
        return TypeToken.of(Double.class);
      case DECIMAL:
        return TypeToken.of(BigDecimal.class);

        // lists and sets: translate to Json arrays
      case LIST:
        return TypeTokens.listOf(getFieldType(field, cqlType.getTypeArguments().get(0)));
      case SET:
        return TypeTokens.setOf(getFieldType(field, cqlType.getTypeArguments().get(0)));

        // maps: translate to Json objects
      case MAP:
        return TypeTokens.mapOf(
            getFieldType(field, cqlType.getTypeArguments().get(0)),
            getFieldType(field, cqlType.getTypeArguments().get(1)));

        // all other types translate to Json strings
      default:
        return TypeToken.of(String.class);
    }
  }
}
