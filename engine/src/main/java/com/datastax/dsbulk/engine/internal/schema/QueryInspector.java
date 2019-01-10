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
import com.datastax.dsbulk.commons.generated.cql3.CqlBaseVisitor;
import com.datastax.dsbulk.commons.generated.cql3.CqlLexer;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.AllowedFunctionNameContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.CfNameContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.CidentContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.ColumnFamilyNameContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.ColumnOperationContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.ColumnOperationDifferentiatorContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.CqlStatementContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.DeleteStatementContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.FunctionContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.InsertStatementContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.IntValueContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.JsonInsertStatementContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.KeyspaceNameContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.KsNameContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.NoncolIdentContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.NormalInsertStatementContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.RelationContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.RelationTypeContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.SelectClauseContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.SelectStatementContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.SelectorContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.TermContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.UnaliasedSelectorContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.UpdateStatementContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.UsingClauseDeleteContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.UsingClauseObjectiveContext;
import com.datastax.dsbulk.commons.generated.cql3.CqlParser.ValueContext;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class QueryInspector extends CqlBaseVisitor<CQLFragment> {

  public static final CQLIdentifier INTERNAL_TIMESTAMP_VARNAME =
      CQLIdentifier.fromInternal("[timestamp]");
  public static final CQLIdentifier INTERNAL_TTL_VARNAME = CQLIdentifier.fromInternal("[ttl]");
  private static final CQLIdentifier INTERNAL_TOKEN_VARNAME =
      CQLIdentifier.fromInternal("partition key token");

  private static final CQLIdentifier QUESTION_MARK = CQLIdentifier.fromInternal("?");
  private static final CQLIdentifier WRITETIME = CQLIdentifier.fromInternal("writetime");
  private static final CQLIdentifier TTL = CQLIdentifier.fromInternal("ttl");

  private final String query;

  // can't use Guava's immutable builders here as some map keys may appear twice in the query
  private final Map<CQLFragment, CQLFragment> resultSetVariablesBuilder = new LinkedHashMap<>();
  private final Map<CQLIdentifier, CQLFragment> assignmentsBuilder = new LinkedHashMap<>();
  private final Set<CQLFragment> writeTimeVariablesBuilder = new LinkedHashSet<>();

  private final ImmutableMap<CQLFragment, CQLFragment> resultSetVariables;
  private final ImmutableMap<CQLIdentifier, CQLFragment> assignments;
  private final ImmutableSet<CQLFragment> writeTimeVariables;

  private CQLIdentifier keyspaceName;
  private CQLIdentifier tableName;
  private int fromClauseStartIndex = -1;
  private int fromClauseEndIndex = -1;
  private boolean hasWhereClause = false;
  private CQLIdentifier tokenRangeRestrictionStartVariable;
  private CQLIdentifier tokenRangeRestrictionEndVariable;
  private int tokenRangeRestrictionVariableIndex = 0;
  private int tokenRangeRestrictionStartVariableIndex = -1;
  private int tokenRangeRestrictionEndVariableIndex = -1;
  private boolean selectStar = false;
  private boolean hasUnsupportedSelectors = false;
  private CQLIdentifier usingTimestampVariable;
  private CQLIdentifier usingTTLVariable;

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
    CqlStatementContext statement = parser.cqlStatement();
    visit(statement);
    resultSetVariables = ImmutableMap.copyOf(resultSetVariablesBuilder);
    assignments = ImmutableMap.copyOf(assignmentsBuilder);
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
   * @return a map of assignments found in the query, from column to value. Only used for write
   *     queries (INSERT, UPDATE, DELETE).
   */
  public ImmutableMap<CQLIdentifier, CQLFragment> getAssignments() {
    return assignments;
  }

  /**
   * @return a map of result set variables found in the query, from unaliased to aliased (if no
   *     alias is present, the aliased form is identical to the unaliased one). This map includes
   *     only regular column selections (e.g. SELECT col1) and function call selections (e.g. SELECT
   *     token(pk)). Only used for read queries (SELECT) under protocol V1, to infer the contents of
   *     the result set.
   */
  public ImmutableMap<CQLFragment, CQLFragment> getResultSetVariables() {
    return resultSetVariables;
  }

  /**
   * @return true if the statement is a SELECT statement with a 'select all' clause ({@code SELECT
   *     *}).
   */
  public boolean isSelectStar() {
    return selectStar;
  }

  /**
   * @return true if the statement is a SELECT statement and at least one of the selectors cannot be
   *     fully parsed by dsbulk (i.e., not a regular column selection nor a ttl/writetime function
   *     call). This is usually only useful in protocol V1, because prepared statements do not
   *     return result set metadata, and dsbulk needs to rely on its own parsing of the query to
   *     infer the result set variables.
   */
  public boolean hasUnsupportedSelectors() {
    return hasUnsupportedSelectors;
  }

  /**
   * @return the variable names found in the query in a USING TIMESTAMP clause, or in the SELECT
   *     clause where the selector is a WRITETIME function call, or empty if none was found. If the
   *     WRITETIME function call is aliased, the alias will appear here.
   */
  public ImmutableSet<CQLFragment> getWriteTimeVariables() {
    return writeTimeVariables;
  }

  /**
   * @return the variable name used in a USING TIMESTAMP clause; or none if no such clause or no
   *     such variable.
   */
  public Optional<CQLIdentifier> getUsingTimestampVariable() {
    return Optional.ofNullable(usingTimestampVariable);
  }

  /**
   * @return the variable name used in a USING TTL clause; or none if no such clause or no such
   *     variable.
   */
  public Optional<CQLIdentifier> getUsingTTLVariable() {
    return Optional.ofNullable(usingTTLVariable);
  }

  /**
   * @return the start index of the FROM clause of a SELECT statement; or -1 if the statement is not
   *     a SELECT.
   */
  public int getFromClauseStartIndex() {
    return fromClauseStartIndex;
  }

  /**
   * @return the end index of the FROM clause of a SELECT statement; or -1 if the statement is not a
   *     SELECT.
   */
  public int getFromClauseEndIndex() {
    return fromClauseEndIndex;
  }

  /** @return true if the statement contains a WHERE clause, false otherwise. */
  public boolean hasWhereClause() {
    return hasWhereClause;
  }

  /**
   * @return the variable name used to define a token range start, e.g. if the restriction is {@code
   *     token(...) > :start}, this method will report {@code start}.
   */
  public Optional<CQLIdentifier> getTokenRangeRestrictionStartVariable() {
    return Optional.ofNullable(tokenRangeRestrictionStartVariable);
  }

  /**
   * @return the variable name used to define a token range end, e.g. if the restriction is {@code
   *     token(...) <= :end}, this method will report {@code end}.
   */
  public Optional<CQLIdentifier> getTokenRangeRestrictionEndVariable() {
    return Optional.ofNullable(tokenRangeRestrictionEndVariable);
  }

  /**
   * @return the zero-based index of the start variable of a token range restriction; or -1 if no
   *     such variable is present.
   */
  public int getTokenRangeRestrictionStartVariableIndex() {
    return tokenRangeRestrictionStartVariableIndex;
  }

  /**
   * @return the zero-based index of the end variable of a token range restriction; or -1 if no such
   *     variable is present.
   */
  public int getTokenRangeRestrictionEndVariableIndex() {
    return tokenRangeRestrictionEndVariableIndex;
  }

  // INSERT

  @Override
  public CQLFragment visitInsertStatement(InsertStatementContext ctx) {
    visitColumnFamilyName(ctx.columnFamilyName());
    return visitChildren(ctx);
  }

  @Override
  public CQLFragment visitNormalInsertStatement(NormalInsertStatementContext ctx) {
    if (ctx.cident().size() != ctx.term().size()) {
      throw new BulkConfigurationException(
          String.format(
              "Invalid query: the number of columns to insert (%d) does not match the number of terms (%d): %s.",
              ctx.cident().size(), ctx.term().size(), query));
    }
    for (int i = 0; i < ctx.cident().size(); i++) {
      CQLIdentifier column = visitCident(ctx.cident().get(i));
      CQLFragment variable = visitTerm(ctx.term().get(i));
      assignmentsBuilder.put(column, variable == QUESTION_MARK ? column : variable);
    }
    if (ctx.usingClause() != null) {
      visitUsingClause(ctx.usingClause());
    }
    return null;
  }

  @Override
  public CQLFragment visitJsonInsertStatement(JsonInsertStatementContext ctx) {
    throw new BulkConfigurationException(
        String.format("Invalid query: INSERT JSON is not supported: %s.", query));
  }

  // UPDATE

  @Override
  public CQLFragment visitUpdateStatement(UpdateStatementContext ctx) {
    visitColumnFamilyName(ctx.columnFamilyName());
    for (ColumnOperationContext op : ctx.columnOperation()) {
      CQLIdentifier column = visitCident(op.cident());
      CQLFragment variable = visitColumnOperationDifferentiator(op.columnOperationDifferentiator());
      if (variable != null) {
        assignmentsBuilder.put(column, variable == QUESTION_MARK ? column : variable);
      }
    }
    visitWhereClause(ctx.whereClause());
    if (ctx.usingClause() != null) {
      visitUsingClause(ctx.usingClause());
    }
    return null;
  }

  @Override
  public CQLFragment visitColumnOperationDifferentiator(ColumnOperationDifferentiatorContext ctx) {
    if (ctx.normalColumnOperation() != null) {
      // normal update operation: column = :variable
      return visitTerm(ctx.normalColumnOperation().term());
    } else if (ctx.shorthandColumnOperation() != null) {
      // shorthand update operation: column += :variable
      return visitTerm(ctx.shorthandColumnOperation().term());
    }
    // unsupported update operation
    return null;
  }

  // SELECT

  @Override
  public CQLFragment visitSelectStatement(SelectStatementContext ctx) {
    if (ctx.K_JSON() != null) {
      throw new BulkConfigurationException(
          String.format("Invalid query: SELECT JSON is not supported: %s.", query));
    }
    visitColumnFamilyName(ctx.columnFamilyName());
    fromClauseStartIndex = ctx.K_FROM().getSymbol().getStartIndex();
    fromClauseEndIndex = ctx.columnFamilyName().getStop().getStopIndex();
    if (ctx.whereClause() != null) {
      hasWhereClause = true;
      visitWhereClause(ctx.whereClause());
    }
    return visitSelectClause(ctx.selectClause());
  }

  @Override
  public CQLFragment visitSelectClause(SelectClauseContext ctx) {
    if (ctx.getText().equals("*")) {
      selectStar = true;
    }
    return super.visitSelectClause(ctx);
  }

  @Override
  @Nullable
  public CQLFragment visitSelector(SelectorContext ctx) {
    CQLFragment unaliased = visitUnaliasedSelector(ctx.unaliasedSelector());
    if (unaliased != null) {
      boolean hasAlias = ctx.noncolIdent() != null;
      CQLFragment alias = hasAlias ? visitNoncolIdent(ctx.noncolIdent()) : unaliased;
      resultSetVariablesBuilder.put(unaliased, alias);
      if (unaliased instanceof FunctionCall) {
        FunctionCall function = (FunctionCall) unaliased;
        if (function.getFunctionName().equals(WRITETIME)) {
          // store the alias since it's the alias that will be returned in the result set
          writeTimeVariablesBuilder.add(alias);
        }
      }
    } else {
      hasUnsupportedSelectors = true;
    }
    return unaliased;
  }

  @Override
  @Nullable
  public CQLFragment visitUnaliasedSelector(UnaliasedSelectorContext ctx) {
    if (!ctx.fident().isEmpty()) {
      // UDT field selection: unsupported
      return null;
    }
    if (ctx.getChildCount() == 1 && ctx.cident() != null) {
      // regular column selection
      return visitCident(ctx.cident());
    } else if (ctx.K_WRITETIME() != null) {
      return new FunctionCall(null, WRITETIME, visitCident(ctx.cident()));
    } else if (ctx.K_TTL() != null) {
      return new FunctionCall(null, TTL, visitCident(ctx.cident()));
    } else if (ctx.functionName() != null) {
      // function calls
      CQLIdentifier keyspaceName = null;
      if (ctx.functionName().keyspaceName() != null) {
        keyspaceName = visitKeyspaceName(ctx.functionName().keyspaceName());
      }
      CQLIdentifier functionName =
          visitAllowedFunctionName(ctx.functionName().allowedFunctionName());
      List<CQLFragment> args = new ArrayList<>();
      if (ctx.selectionFunctionArgs() != null) {
        for (UnaliasedSelectorContext arg : ctx.selectionFunctionArgs().unaliasedSelector()) {
          CQLFragment term = visitUnaliasedSelector(arg);
          if (term != null) {
            args.add(term);
          } else {
            // unknown argument type
            return null;
          }
        }
      }
      return new FunctionCall(keyspaceName, functionName, args);
    }
    // other selectors: unsupported
    return null;
  }

  // DELETE

  @Override
  @Nullable
  public CQLFragment visitDeleteStatement(DeleteStatementContext ctx) {
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
  @Nullable
  public CQLFragment visitRelation(RelationContext ctx) {
    // relation contains another relation: drill down
    while (ctx.relation() != null) {
      ctx = ctx.relation();
    }
    // we only inspect WHERE clauses for UPDATE and DELETE
    // and only care about primary key equality constraints such as
    // myCol = :myVar or myCol = ?
    if (ctx.getChildCount() == 3
        && ctx.getChild(0) instanceof CidentContext
        && ctx.getChild(1) instanceof RelationTypeContext
        && ctx.getChild(2) instanceof TermContext
        && ctx.getChild(1).getText().equals("=")) {
      CQLIdentifier column = visitCident(ctx.cident());
      CQLFragment variable = visitTerm(ctx.term().get(0));
      assignmentsBuilder.put(column, variable.equals(QUESTION_MARK) ? column : variable);
    } else if (ctx.K_TOKEN() != null) {
      CQLFragment variable = visitTerm(ctx.term().get(0));
      if (variable instanceof CQLIdentifier) {
        if (variable == QUESTION_MARK) {
          variable = INTERNAL_TOKEN_VARNAME;
        }
        if (ctx.relationType().getText().equals(">")) {
          tokenRangeRestrictionStartVariable = (CQLIdentifier) variable;
          tokenRangeRestrictionStartVariableIndex = tokenRangeRestrictionVariableIndex++;
        } else if (ctx.relationType().getText().equals("<=")) {
          tokenRangeRestrictionEndVariable = (CQLIdentifier) variable;
          tokenRangeRestrictionEndVariableIndex = tokenRangeRestrictionVariableIndex++;
        }
      }
    }
    // other relation types: unsupported
    return null;
  }

  // TERMS AND VALUES

  @Override
  @NotNull
  public CQLFragment visitTerm(TermContext ctx) {
    // ( comparator ) term
    while (ctx.term() != null) {
      ctx = ctx.term();
    }
    // term is a value
    if (ctx.value() != null) {
      return visitValue(ctx.value());
    } else {
      // term is a function
      assert ctx.function() != null;
      return visitFunction(ctx.function());
    }
  }

  @Override
  @NotNull
  public FunctionCall visitFunction(FunctionContext ctx) {
    CQLIdentifier keyspaceName = null;
    if (ctx.functionName().keyspaceName() != null) {
      keyspaceName = visitKeyspaceName(ctx.functionName().keyspaceName());
    }
    CQLIdentifier functionName = visitAllowedFunctionName(ctx.functionName().allowedFunctionName());
    List<CQLFragment> args = new ArrayList<>();
    if (ctx.functionArgs() != null) {
      for (TermContext arg : ctx.functionArgs().term()) {
        CQLFragment term = visitTerm(arg);
        if (term == QUESTION_MARK) {
          throw new BulkConfigurationException(
              String.format(
                  "Invalid query: positional variables are not allowed as function parameters: %s.",
                  query));
        }
        args.add(term);
      }
    }
    return new FunctionCall(keyspaceName, functionName, args);
  }

  @Override
  @NotNull
  public CQLFragment visitValue(ValueContext ctx) {
    // value is a positional bind marker
    if (ctx.QMARK() != null) {
      return QUESTION_MARK;
    }
    // value is a named bound variable
    if (ctx.noncolIdent() != null) {
      return visitNoncolIdent(ctx.noncolIdent());
    }
    return new CQLLiteral(ctx.getText());
  }

  // IDENTIFIERS

  @Override
  @NotNull
  public CQLFragment visitColumnFamilyName(ColumnFamilyNameContext ctx) {
    if (ctx.ksName() != null) {
      keyspaceName = visitKsName(ctx.ksName());
    }
    tableName = visitCfName(ctx.cfName());
    return tableName;
  }

  @Override
  @NotNull
  public CQLIdentifier visitAllowedFunctionName(AllowedFunctionNameContext ctx) {
    if (ctx.QUOTED_NAME() != null) {
      return CQLIdentifier.fromCql(ctx.getText());
    } else {
      return CQLIdentifier.fromInternal(ctx.getText().toLowerCase());
    }
  }

  @Override
  public CQLIdentifier visitKeyspaceName(KeyspaceNameContext ctx) {
    return visitKsName(ctx.ksName());
  }

  @Override
  @NotNull
  public CQLIdentifier visitKsName(KsNameContext ctx) {
    if (ctx.QUOTED_NAME() != null) {
      return CQLIdentifier.fromCql(ctx.getText());
    } else {
      return CQLIdentifier.fromInternal(ctx.getText().toLowerCase());
    }
  }

  @Override
  @NotNull
  public CQLIdentifier visitCfName(CfNameContext ctx) {
    if (ctx.QUOTED_NAME() != null) {
      return CQLIdentifier.fromCql(ctx.getText());
    } else {
      return CQLIdentifier.fromInternal(ctx.getText().toLowerCase());
    }
  }

  @Override
  @NotNull
  public CQLIdentifier visitCident(CidentContext ctx) {
    if (ctx.QUOTED_NAME() != null) {
      return CQLIdentifier.fromCql(ctx.getText());
    } else {
      return CQLIdentifier.fromInternal(ctx.getText().toLowerCase());
    }
  }

  @Override
  @NotNull
  public CQLIdentifier visitNoncolIdent(NoncolIdentContext ctx) {
    if (ctx.QUOTED_NAME() != null) {
      return CQLIdentifier.fromCql(ctx.getText());
    } else {
      return CQLIdentifier.fromInternal(ctx.getText().toLowerCase());
    }
  }

  // USING TIMESTAMP AND TTL

  @Override
  @Nullable
  public CQLFragment visitUsingClauseObjective(UsingClauseObjectiveContext ctx) {
    if (ctx.K_TIMESTAMP() != null) {
      return visitUsingTimestamp(ctx.intValue());
    } else {
      assert ctx.K_TTL() != null;
      return visitUsingTTL(ctx.intValue());
    }
  }

  @Override
  @Nullable
  public CQLFragment visitUsingClauseDelete(UsingClauseDeleteContext ctx) {
    return visitUsingTimestamp(ctx.intValue());
  }

  @Nullable
  private CQLIdentifier visitUsingTimestamp(IntValueContext intValueContext) {
    if (intValueContext.noncolIdent() != null) {
      CQLIdentifier variable = visitNoncolIdent(intValueContext.noncolIdent());
      writeTimeVariablesBuilder.add(variable);
      usingTimestampVariable = variable;
    } else if (intValueContext.QMARK() != null) {
      writeTimeVariablesBuilder.add(INTERNAL_TIMESTAMP_VARNAME);
      usingTimestampVariable = INTERNAL_TIMESTAMP_VARNAME;
    }
    return usingTimestampVariable;
  }

  @Nullable
  private CQLIdentifier visitUsingTTL(IntValueContext intValueContext) {
    if (intValueContext.noncolIdent() != null) {
      usingTTLVariable = visitNoncolIdent(intValueContext.noncolIdent());
    } else if (intValueContext.QMARK() != null) {
      usingTTLVariable = INTERNAL_TTL_VARNAME;
    }
    return usingTTLVariable;
  }
}
