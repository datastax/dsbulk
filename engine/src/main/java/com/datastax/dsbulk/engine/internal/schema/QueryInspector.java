/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.cql3.CqlBaseVisitor;
import com.datastax.dsbulk.commons.cql3.CqlLexer;
import com.datastax.dsbulk.commons.cql3.CqlParser;
import com.datastax.dsbulk.commons.cql3.CqlParser.AllowedFunctionNameContext;
import com.datastax.dsbulk.commons.cql3.CqlParser.FunctionContext;
import com.datastax.dsbulk.commons.cql3.CqlParser.FunctionNameContext;
import com.datastax.dsbulk.commons.cql3.CqlParser.TermContext;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

public class QueryInspector extends CqlBaseVisitor<CQLFragment> {

  private static final CQLIdentifier QUESTION_MARK = CQLIdentifier.fromInternal("?");
  private static final CQLIdentifier WRITETIME = CQLIdentifier.fromInternal("writetime");

  private final String query;

  // can't use Guava's immutable builders here as some map keys may appear twice in the query
  private final Map<CQLIdentifier, CQLFragment> resultSetVariablesBuilder = new LinkedHashMap<>();
  private final Map<CQLIdentifier, CQLFragment> boundVariablesBuilder = new LinkedHashMap<>();
  private final Set<CQLFragment> writeTimeVariablesBuilder = new LinkedHashSet<>();

  private final ImmutableMap<CQLIdentifier, CQLFragment> resultSetVariables;
  private final ImmutableMap<CQLIdentifier, CQLFragment> boundVariables;
  private final ImmutableSet<CQLFragment> writeTimeVariables;

  private CQLIdentifier keyspaceName;
  private CQLIdentifier tableName;

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
    resultSetVariables = ImmutableMap.copyOf(resultSetVariablesBuilder);
    boundVariables = ImmutableMap.copyOf(boundVariablesBuilder);
    writeTimeVariables = ImmutableSet.copyOf(writeTimeVariablesBuilder);
  }

  /** @return the keyspace name found in the query, or empty if none was found. */
  public Optional<CQLIdentifier> getKeyspaceName() {
    return Optional.ofNullable(keyspaceName);
  }

  /** @return the table name found in the query; never {@code null}. */
  public CQLIdentifier getTableName() {
    return tableName;
  }

  /**
   * @return a map of all bound variables found in the query, from column name to variable name.
   *     Only used for write queries (INSERT, UPDATE, DELETE). This map does not include functions.
   */
  public ImmutableMap<CQLIdentifier, CQLFragment> getBoundVariables() {
    return boundVariables;
  }

  /**
   * @return a map of all result set variables found in the query, from column name to alias (if no
   *     alias is present, the column name maps to itself). Only used for read queries (SELECT).
   *     This map does not include functions.
   */
  public ImmutableMap<CQLIdentifier, CQLFragment> getResultSetVariables() {
    return resultSetVariables;
  }

  /**
   * @return the variable names found in the query in a USING TIMESTAMP clause, or in the SELECT
   *     clause where the selector is a WRITETIME function call, or empty if none was found. If the
   *     WRITETIME function call is aliased, the alias will appear here.
   */
  public ImmutableSet<CQLFragment> getWriteTimeVariables() {
    return writeTimeVariables;
  }

  // INSERT

  @Override
  public CQLFragment visitInsertStatement(CqlParser.InsertStatementContext ctx) {
    visitColumnFamilyName(ctx.columnFamilyName());
    return visitChildren(ctx);
  }

  @Override
  public CQLFragment visitNormalInsertStatement(CqlParser.NormalInsertStatementContext ctx) {
    if (ctx.cident().size() != ctx.term().size()) {
      throw new BulkConfigurationException(
          String.format(
              "Invalid query: the number of columns to insert (%d) does not match the number of terms (%d): %s.",
              ctx.cident().size(), ctx.term().size(), query));
    }
    for (int i = 0; i < ctx.cident().size(); i++) {
      CQLIdentifier column = visitCident(ctx.cident().get(i));
      CQLFragment variable = visitTerm(ctx.term().get(i));
      if (variable != null) {
        boundVariablesBuilder.put(column, variable == QUESTION_MARK ? column : variable);
      }
    }
    if (ctx.usingClause() != null) {
      visitUsingClause(ctx.usingClause());
    }
    return null;
  }

  @Override
  public CQLFragment visitJsonInsertStatement(CqlParser.JsonInsertStatementContext ctx) {
    throw new BulkConfigurationException(
        String.format("Invalid query: INSERT JSON is not supported: %s.", query));
  }

  // UPDATE

  @Override
  public CQLFragment visitUpdateStatement(CqlParser.UpdateStatementContext ctx) {
    visitColumnFamilyName(ctx.columnFamilyName());
    for (CqlParser.ColumnOperationContext op : ctx.columnOperation()) {
      CQLIdentifier column = visitCident(op.cident());
      CQLFragment variable = visitColumnOperationDifferentiator(op.columnOperationDifferentiator());
      if (variable != null) {
        boundVariablesBuilder.put(column, variable == QUESTION_MARK ? column : variable);
      }
    }
    visitWhereClause(ctx.whereClause());
    if (ctx.usingClause() != null) {
      visitUsingClause(ctx.usingClause());
    }
    return null;
  }

  @Override
  public CQLFragment visitColumnOperationDifferentiator(
      CqlParser.ColumnOperationDifferentiatorContext ctx) {
    if (ctx.normalColumnOperation() != null) {
      // normal update operation: column = :variable
      return visitTerm(ctx.normalColumnOperation().term());
    } else if (ctx.shorthandColumnOperation() != null) {
      // shorthand update operation: column += :variable
      return visitTerm(ctx.shorthandColumnOperation().term());
    } else {
      // unsupported update operation
      return null;
    }
  }

  // SELECT

  @Override
  public CQLFragment visitSelectStatement(CqlParser.SelectStatementContext ctx) {
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
  public CQLFragment visitSelector(CqlParser.SelectorContext ctx) {
    if (ctx.unaliasedSelector().getChildCount() == 1 && ctx.unaliasedSelector().cident() != null) {
      // selection of a column, possibly aliased
      CQLIdentifier column = visitCident(ctx.unaliasedSelector().cident());
      if (ctx.noncolIdent() != null) {
        CQLIdentifier alias = visitNoncolIdent(ctx.noncolIdent());
        resultSetVariablesBuilder.put(column, alias);
      } else {
        resultSetVariablesBuilder.put(column, column);
      }
    } else if (ctx.unaliasedSelector().K_WRITETIME() != null) {
      CQLFragment writetime;
      if (ctx.noncolIdent() != null) {
        writetime = visitNoncolIdent(ctx.noncolIdent());
      } else {
        writetime = new FunctionCall(WRITETIME, visitCident(ctx.unaliasedSelector().cident()));
      }
      writeTimeVariablesBuilder.add(writetime);
    }
    // other selector types: unsupported
    return null;
  }

  // DELETE

  @Override
  public CQLFragment visitDeleteStatement(CqlParser.DeleteStatementContext ctx) {
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
  public CQLFragment visitRelation(CqlParser.RelationContext ctx) {
    // relation contains another relation: drill down
    while (ctx.relation() != null) {
      ctx = ctx.relation();
    }
    // we only inspect WHERE clauses for UPDATE and DELETE
    // and only care about primary key equality constraints such as
    // myCol = :myVar or myCol = ?
    if (ctx.getChildCount() == 3
        && ctx.getChild(0) instanceof CqlParser.CidentContext
        && ctx.getChild(1) instanceof CqlParser.RelationTypeContext
        && ctx.getChild(2) instanceof CqlParser.TermContext
        && ctx.getChild(1).getText().equals("=")) {
      CQLIdentifier column = visitCident(ctx.cident());
      CQLFragment variable = visitTerm(ctx.term().get(0));
      if (variable != null) {
        boundVariablesBuilder.put(column, variable.equals(QUESTION_MARK) ? column : variable);
      }
    }
    // other relation types: unsupported
    return null;
  }

  // TERMS AND VALUES

  @Override
  public CQLFragment visitTerm(CqlParser.TermContext ctx) {
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
      return visitFunction(ctx.function());
    }
    // other terms: unsupported
    return null;
  }

  @Override
  public FunctionCall visitFunction(FunctionContext ctx) {
    CQLIdentifier functionName = visitFunctionName(ctx.functionName());
    List<CQLFragment> args = new ArrayList<>();
    if (ctx.functionArgs() != null) {
      for (TermContext arg : ctx.functionArgs().term()) {
        CQLFragment term = visitTerm(arg);
        args.add(Objects.requireNonNull(term));
      }
    }
    return new FunctionCall(functionName, args);
  }

  @Override
  public CQLIdentifier visitFunctionName(FunctionNameContext ctx) {
    if (ctx.keyspaceName() != null) {
      throw new BulkConfigurationException(
          "Invalid query: qualified functions in SELECT clause are not supported: "
              + ctx.getText());
    }
    return visitAllowedFunctionName(ctx.allowedFunctionName());
  }

  @Override
  public CQLIdentifier visitValue(CqlParser.ValueContext ctx) {
    // value is a positional bind marker
    if (ctx.QMARK() != null) {
      return QUESTION_MARK;
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
  public CQLFragment visitColumnFamilyName(CqlParser.ColumnFamilyNameContext ctx) {
    if (ctx.ksName() != null) {
      keyspaceName = visitKsName(ctx.ksName());
    }
    tableName = visitCfName(ctx.cfName());
    return null;
  }

  @Override
  public CQLIdentifier visitAllowedFunctionName(AllowedFunctionNameContext ctx) {
    if (ctx.QUOTED_NAME() != null) {
      return CQLIdentifier.fromCql(ctx.getText());
    } else {
      return CQLIdentifier.fromInternal(ctx.getText().toLowerCase());
    }
  }

  @Override
  public CQLIdentifier visitKsName(CqlParser.KsNameContext ctx) {
    if (ctx.QUOTED_NAME() != null) {
      return CQLIdentifier.fromCql(ctx.getText());
    } else {
      return CQLIdentifier.fromInternal(ctx.getText().toLowerCase());
    }
  }

  @Override
  public CQLIdentifier visitCfName(CqlParser.CfNameContext ctx) {
    if (ctx.QUOTED_NAME() != null) {
      return CQLIdentifier.fromCql(ctx.getText());
    } else {
      return CQLIdentifier.fromInternal(ctx.getText().toLowerCase());
    }
  }

  @Override
  public CQLIdentifier visitCident(CqlParser.CidentContext ctx) {
    if (ctx.QUOTED_NAME() != null) {
      return CQLIdentifier.fromCql(ctx.getText());
    } else {
      return CQLIdentifier.fromInternal(ctx.getText().toLowerCase());
    }
  }

  @Override
  public CQLIdentifier visitNoncolIdent(CqlParser.NoncolIdentContext ctx) {
    if (ctx.QUOTED_NAME() != null) {
      return CQLIdentifier.fromCql(ctx.getText());
    } else {
      return CQLIdentifier.fromInternal(ctx.getText().toLowerCase());
    }
  }

  // USING TIMESTAMP AND TTL

  @Override
  public CQLFragment visitUsingClauseObjective(CqlParser.UsingClauseObjectiveContext ctx) {
    if (ctx.K_TIMESTAMP() != null) {
      visitUsingTimestamp(ctx.intValue());
    } else if (ctx.K_TTL() != null) {
      visitUsingTTL(ctx.intValue());
    }
    return null;
  }

  @Override
  public CQLFragment visitUsingClauseDelete(CqlParser.UsingClauseDeleteContext ctx) {
    visitUsingTimestamp(ctx.intValue());
    return null;
  }

  private void visitUsingTimestamp(CqlParser.IntValueContext intValueContext) {
    if (intValueContext.noncolIdent() != null) {
      writeTimeVariablesBuilder.add(visitNoncolIdent(intValueContext.noncolIdent()));
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
