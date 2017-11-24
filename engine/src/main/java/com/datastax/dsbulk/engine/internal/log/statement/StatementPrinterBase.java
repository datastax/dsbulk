/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.log.statement;

import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatVerbosity.EXTENDED;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatVerbosity.NORMAL;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.consistencyLevel;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.defaultTimestamp;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.idempotent;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.listElementSeparator;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.readTimeoutMillis;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.serialConsistencyLevel;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.summaryEnd;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.summaryStart;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.unsetValue;

import com.datastax.driver.core.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A common parent class for {@link StatementPrinter} implementations.
 *
 * <p>This class assumes a common formatting pattern comprised of the following sections:
 *
 * <ol>
 *   <li>Header: this section should contain two subsections:
 *       <ol>
 *         <li>The actual statement class and the statement's hash code;
 *         <li>The statement "summary"; examples of typical information that could be included here
 *             are: the statement's consistency level; its default timestamp; its idempotence flag;
 *             the number of bound values; etc.
 *       </ol>
 *   <li>Query String: this section should print the statement's query string, if it is available;
 *       this section is only enabled if the verbosity is {@link StatementFormatVerbosity#NORMAL
 *       NORMAL} or higher;
 *   <li>Bound Values: this section should print the statement's bound values, if available; this
 *       section is only enabled if the verbosity is {@link StatementFormatVerbosity#EXTENDED
 *       EXTENDED};
 *   <li>Footer: an optional section, empty by default.
 * </ol>
 */
public abstract class StatementPrinterBase<S extends Statement> implements StatementPrinter<S> {

  @Override
  public abstract Class<? extends Statement> getSupportedStatementClass();

  @Override
  public void print(S statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    printHeader(statement, out, verbosity);
    if (verbosity.compareTo(NORMAL) >= 0) {
      printQueryString(statement, out, verbosity);
      if (verbosity.compareTo(EXTENDED) >= 0) {
        printBoundValues(statement, out, verbosity);
      }
    }
  }

  protected void printHeader(S statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    out.appendClassNameAndHashCode(statement);
    List<String> properties = collectStatementProperties(statement, out, verbosity);
    if (properties != null && !properties.isEmpty()) {
      out.append(summaryStart);
      Iterator<String> it = properties.iterator();
      while (it.hasNext()) {
        String property = it.next();
        out.append(property);
        if (it.hasNext()) out.append(listElementSeparator);
      }
      out.append(summaryEnd);
    }
  }

  protected List<String> collectStatementProperties(
      S statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    List<String> properties = new ArrayList<>();
    if (verbosity.compareTo(NORMAL) > 0) {
      if (statement.isIdempotent() != null)
        properties.add(String.format(idempotent, statement.isIdempotent()));
      else properties.add(String.format(idempotent, unsetValue));
      if (statement.getConsistencyLevel() != null)
        properties.add(String.format(consistencyLevel, statement.getConsistencyLevel()));
      else properties.add(String.format(consistencyLevel, unsetValue));
      if (statement.getSerialConsistencyLevel() != null)
        properties.add(
            String.format(serialConsistencyLevel, statement.getSerialConsistencyLevel()));
      else properties.add(String.format(serialConsistencyLevel, unsetValue));
      if (statement.getDefaultTimestamp() != Long.MIN_VALUE)
        properties.add(String.format(defaultTimestamp, statement.getDefaultTimestamp()));
      else properties.add(String.format(defaultTimestamp, unsetValue));
      if (statement.getReadTimeoutMillis() != Integer.MIN_VALUE)
        properties.add(String.format(readTimeoutMillis, statement.getReadTimeoutMillis()));
      else properties.add(String.format(readTimeoutMillis, unsetValue));
    }
    return properties;
  }

  protected void printQueryString(
      S statement, StatementWriter out, StatementFormatVerbosity verbosity) {}

  protected void printBoundValues(
      S statement, StatementWriter out, StatementFormatVerbosity verbosity) {}
}
