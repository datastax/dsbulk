/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.workflow.commons.format.statement;

import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.format.statement.StatementWriter;
import com.datastax.oss.dsbulk.workflow.commons.log.LogManagerUtils;
import com.datastax.oss.dsbulk.workflow.commons.statement.BulkStatement;

public interface BulkStatementPrinter {

  default void appendRecord(BulkStatement<Record> statement, StatementWriter out) {
    Record record = statement.getSource();
    out.newLine()
        .indent()
        .append("Resource: ")
        .append(String.valueOf(record.getResource()))
        .newLine()
        .indent()
        .append("Position: ")
        .append(String.valueOf(record.getPosition()))
        .newLine()
        .indent()
        .append("Source: ")
        .append(LogManagerUtils.formatSource(record));
  }
}
