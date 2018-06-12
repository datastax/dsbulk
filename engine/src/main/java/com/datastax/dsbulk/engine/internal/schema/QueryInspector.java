/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import com.datastax.driver.core.DriverCoreHooks;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.cql3.CqlBaseVisitor;
import com.datastax.dsbulk.commons.cql3.CqlLexer;
import com.datastax.dsbulk.commons.cql3.CqlParser;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import java.util.LinkedHashMap;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

public class QueryInspector extends CqlBaseVisitor<String> {

  private final String query;

  private final LinkedHashMap<String, String> columnsToVariablesBuilder = new LinkedHashMap<>();

  private final BiMap<String, String> columnsToVariables;

  private String currentColumn;
  private String keyspaceName;
  private String tableName;
  private String writeTimeVariable;

  public QueryInspector(String query) {
    this.query = query;
    CodePointCharStream input = CharStreams.fromString(query);
    CqlLexer lexer = new CqlLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    CqlParser parser = new CqlParser(tokens);
    BaseErrorListener listener =
        new BaseErrorListener() {
          @Override
          public void syntaxError(
              Recognizer<?, ?> recognizer,
              Object offendingSymbol,
              int line,
              int col,
              String msg,
              RecognitionException e) {
            throw new BulkConfigurationException(
                String.format(
                    "Invalid query: '%s' could not be parsed at line %d:%d: %s",
                    query, line, col, msg),
                e);
          }
        };
    lexer.removeErrorListeners();
    lexer.addErrorListener(listener);
    parser.removeErrorListeners();
    parser.addErrorListener(listener);
    CqlParser.CqlStatementContext statement = parser.cqlStatement();
    visit(statement);
    columnsToVariables = ImmutableBiMap.copyOf(columnsToVariablesBuilder);
  }

  public String getKeyspaceName() {
    return keyspaceName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getWriteTimeVariable() {
    return writeTimeVariable;
  }

  public BiMap<String, String> getColumnsToVariables() {
    return columnsToVariables;
  }

  // INSERT

  @Override
  public String visitInsertStatement(CqlParser.InsertStatementContext ctx) {
    visitColumnFamilyName(ctx.columnFamilyName());
    return visitChildren(ctx);
  }

  @Override
  public String visitNormalInsertStatement(CqlParser.NormalInsertStatementContext ctx) {
    if (ctx.cident().size() != ctx.term().size()) {
      throw new BulkConfigurationException(
          String.format(
              "Invalid query: the number of columns to insert (%d) does not match the number of terms (%d): %s.",
              ctx.cident().size(), ctx.term().size(), query));
    }
    for (int i = 0; i < ctx.cident().size(); i++) {
      currentColumn = visitCident(ctx.cident().get(i));
      String variable = visitTerm(ctx.term().get(i));
      if (variable != null) {
        columnsToVariablesBuilder.put(currentColumn, variable);
      }
    }
    if (ctx.usingClause() != null) {
      visitUsingClause(ctx.usingClause());
    }
    return null;
  }

  @Override
  public String visitJsonInsertStatement(CqlParser.JsonInsertStatementContext ctx) {
    throw new BulkConfigurationException(
        String.format("Invalid query: INSERT JSON is not supported: %s.", query));
  }

  // UPDATE

  @Override
  public String visitUpdateStatement(CqlParser.UpdateStatementContext ctx) {
    visitColumnFamilyName(ctx.columnFamilyName());
    for (CqlParser.ColumnOperationContext op : ctx.columnOperation()) {
      currentColumn = visitCident(op.cident());
      String variable = visitColumnOperationDifferentiator(op.columnOperationDifferentiator());
      if (variable != null) {
        columnsToVariablesBuilder.put(currentColumn, variable);
      }
    }
    visitWhereClause(ctx.whereClause());
    if (ctx.usingClause() != null) {
      visitUsingClause(ctx.usingClause());
    }
    return null;
  }

  @Override
  public String visitColumnOperationDifferentiator(
      CqlParser.ColumnOperationDifferentiatorContext ctx) {
    if (ctx.normalColumnOperation() != null) {
      return visitTerm(ctx.normalColumnOperation().term());
    } else if (ctx.shorthandColumnOperation() != null) {
      return visitTerm(ctx.shorthandColumnOperation().term());
    } else {
      // unsupported update operation
      return null;
    }
  }

  // SELECT

  @Override
  public String visitSelectStatement(CqlParser.SelectStatementContext ctx) {
    if (ctx.K_JSON() != null) {
      throw new BulkConfigurationException(
          String.format("Invalid query: SELECT JSON is not supported: %s.", query));
    }
    // do not inspect WHERE clause, we want only result set variables;
    // if the query has a token range restriction, it will be validated later.
    visitColumnFamilyName(ctx.columnFamilyName());
    return visitSelectClause(ctx.selectClause());
  }

  @Override
  public String visitSelector(CqlParser.SelectorContext ctx) {
    if (ctx.unaliasedSelector().getChildCount() == 1 && ctx.unaliasedSelector().cident() != null) {
      // selection of a column, possibly aliased
      String column = visitCident(ctx.unaliasedSelector().cident());
      if (ctx.noncolIdent() == null) {
        // unaliased selection
        columnsToVariablesBuilder.put(column, column);
      } else {
        // aliased selection
        String variable = visitNoncolIdent(ctx.noncolIdent());
        columnsToVariablesBuilder.put(column, variable);
      }
    }
    // other selector types: unsupported
    return null;
  }

  // DELETE

  @Override
  public String visitDeleteStatement(CqlParser.DeleteStatementContext ctx) {
    visitColumnFamilyName(ctx.columnFamilyName());
    // do not inspect delete selection, only the WHERE clause matters
    visitWhereClause(ctx.whereClause());
    if (ctx.usingClauseDelete() != null) {
      visitUsingClauseDelete(ctx.usingClauseDelete());
    }
    return null;
  }

  // WHERE clause

  @Override
  public String visitRelation(CqlParser.RelationContext ctx) {
    // relation contains another relation: drill down
    while (ctx.relation() != null) {
      ctx = ctx.relation();
    }
    if (ctx.getChildCount() == 3
        && ctx.getChild(0) instanceof CqlParser.CidentContext
        && ctx.getChild(1) instanceof CqlParser.RelationTypeContext
        && ctx.getChild(2) instanceof CqlParser.TermContext) {
      // restriction on a column, as in WHERE col = :value
      currentColumn = visitCident(ctx.cident());
      String variable = visitTerm(ctx.term().get(0));
      if (variable != null) {
        columnsToVariablesBuilder.put(currentColumn, variable);
      }
    }
    // other relation types: unsupported
    return null;
  }

  // TERMS AND VALUES

  @Override
  public String visitTerm(CqlParser.TermContext ctx) {
    // term contains another term: drill down
    while (ctx.term() != null) {
      ctx = ctx.term();
    }
    // term is a value
    if (ctx.value() != null) {
      return visitValue(ctx.value());
    }
    // term is a function
    if (ctx.function() != null) {
      return ctx.getText();
    }
    // other terms: unsupported
    return null;
  }

  @Override
  public String visitValue(CqlParser.ValueContext ctx) {
    // value is a positional bind marker
    if (ctx.QMARK() != null) {
      return currentColumn;
    }
    // value is a named bound variable
    if (ctx.noncolIdent() != null) {
      return visitNoncolIdent(ctx.noncolIdent());
    }
    // other values: unsupported
    return null;
  }

  // IDENTIFIERS

  @Override
  public String visitColumnFamilyName(CqlParser.ColumnFamilyNameContext ctx) {
    if (ctx.ksName() != null) {
      keyspaceName = visitKsName(ctx.ksName());
    }
    tableName = visitCfName(ctx.cfName());
    return null;
  }

  @Override
  public String visitKsName(CqlParser.KsNameContext ctx) {
    return DriverCoreHooks.handleId(ctx.getText());
  }

  @Override
  public String visitCfName(CqlParser.CfNameContext ctx) {
    return DriverCoreHooks.handleId(ctx.getText());
  }

  @Override
  public String visitCident(CqlParser.CidentContext ctx) {
    return DriverCoreHooks.handleId(ctx.getText());
  }

  @Override
  public String visitNoncolIdent(CqlParser.NoncolIdentContext ctx) {
    return DriverCoreHooks.handleId(ctx.getText());
  }

  // USING TIMESTAMP AND TTL

  @Override
  public String visitUsingClauseObjective(CqlParser.UsingClauseObjectiveContext ctx) {
    if (ctx.K_TIMESTAMP() != null) {
      visitUsingTimestamp(ctx.intValue());
    } else if (ctx.K_TTL() != null) {
      visitUsingTTL(ctx.intValue());
    }
    return null;
  }

  @Override
  public String visitUsingClauseDelete(CqlParser.UsingClauseDeleteContext ctx) {
    visitUsingTimestamp(ctx.intValue());
    return null;
  }

  private void visitUsingTimestamp(CqlParser.IntValueContext intValueContext) {
    if (intValueContext.noncolIdent() != null) {
      writeTimeVariable = visitNoncolIdent(intValueContext.noncolIdent());
    } else if (intValueContext.QMARK() != null) {
      throw new BulkConfigurationException(
          String.format(
              "Invalid query: positional variables are not allowed in USING TIMESTAMP clauses, "
                  + "please une named variables instead: %s.",
              query));
    }
  }

  private void visitUsingTTL(CqlParser.IntValueContext intValueContext) {
    if (intValueContext.QMARK() != null) {
      throw new BulkConfigurationException(
          String.format(
              "Invalid query: positional variables are not allowed in USING TTL clauses, "
                  + "please une named variables instead: %s.",
              query));
    }
  }
}
