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
package com.datastax.oss.dsbulk.workflow.commons.schema;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.dsbulk.generated.cql3.CqlBaseVisitor;
import com.datastax.oss.dsbulk.generated.cql3.CqlLexer;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.AllowedFunctionNameContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.CfNameContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.CidentContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.ColumnFamilyNameContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.ColumnOperationContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.ColumnOperationDifferentiatorContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.CqlStatementContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.DeleteStatementContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.FunctionContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.InsertStatementContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.IntValueContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.JsonInsertStatementContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.KeyspaceNameContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.KsNameContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.NoncolIdentContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.NormalInsertStatementContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.RelationContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.RelationTypeContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.SelectClauseContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.SelectStatementContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.SelectorContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.TermContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.UnaliasedSelectorContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.UpdateStatementContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.UsingClauseDeleteContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.UsingClauseObjectiveContext;
import com.datastax.oss.dsbulk.generated.cql3.CqlParser.ValueContext;
import com.datastax.oss.dsbulk.mapping.CQLFragment;
import com.datastax.oss.dsbulk.mapping.CQLLiteral;
import com.datastax.oss.dsbulk.mapping.CQLWord;
import com.datastax.oss.dsbulk.mapping.FunctionCall;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
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

public class QueryInspector extends CqlBaseVisitor<CQLFragment> {

  public static final CQLWord INTERNAL_TIMESTAMP_VARNAME = CQLWord.fromInternal("[timestamp]");
  public static final CQLWord INTERNAL_TTL_VARNAME = CQLWord.fromInternal("[ttl]");
  private static final CQLWord INTERNAL_TOKEN_VARNAME = CQLWord.fromInternal("partition key token");

  private static final CQLWord QUESTION_MARK = CQLWord.fromInternal("?");
  private static final CQLWord WRITETIME = CQLWord.fromInternal("writetime");
  private static final CQLWord TTL = CQLWord.fromInternal("ttl");
  private static final CQLWord SOLR_QUERY = CQLWord.fromInternal("solr_query");

  private final String query;

  // can't use Guava's immutable builders here as some map keys may appear twice in the query
  private final Map<CQLFragment, CQLFragment> resultSetVariablesBuilder = new LinkedHashMap<>();
  private final Map<CQLWord, CQLFragment> assignmentsBuilder = new LinkedHashMap<>();
  private final Set<CQLFragment> writeTimeVariablesBuilder = new LinkedHashSet<>();

  private final ImmutableMap<CQLFragment, CQLFragment> resultSetVariables;
  private final ImmutableMap<CQLWord, CQLFragment> assignments;
  private final ImmutableSet<CQLFragment> writeTimeVariables;

  private CQLWord keyspaceName;
  private CQLWord tableName;
  private int fromClauseStartIndex = -1;
  private int fromClauseEndIndex = -1;
  private boolean hasWhereClause = false;
  private CQLWord tokenRangeRestrictionStartVariable;
  private CQLWord tokenRangeRestrictionEndVariable;
  private int tokenRangeRestrictionVariableIndex = 0;
  private int tokenRangeRestrictionStartVariableIndex = -1;
  private int tokenRangeRestrictionEndVariableIndex = -1;
  private boolean selectStar = false;
  private boolean hasUnsupportedSelectors = false;
  private CQLWord usingTimestampVariable;
  private CQLWord usingTTLVariable;
  private boolean hasSearchClause = false;
  private boolean parallelizable = true;

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
            throw new IllegalArgumentException(
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
  public Optional<CQLWord> getKeyspaceName() {
    return Optional.ofNullable(keyspaceName);
  }

  /** @return the table name found in the query; never {@code null}. */
  public CQLWord getTableName() {
    return tableName;
  }

  /**
   * @return a map of assignments found in the query, from column to value. Only used for write
   *     queries (INSERT, UPDATE, DELETE).
   */
  public ImmutableMap<CQLWord, CQLFragment> getAssignments() {
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
  public Optional<CQLWord> getUsingTimestampVariable() {
    return Optional.ofNullable(usingTimestampVariable);
  }

  /**
   * @return the variable name used in a USING TTL clause; or none if no such clause or no such
   *     variable.
   */
  public Optional<CQLWord> getUsingTTLVariable() {
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

  /** @return whether the SELECT statement has a search clause: WHERE solr_query = ... */
  public boolean hasSearchClause() {
    return hasSearchClause;
  }

  /**
   * @return the variable name used to define a token range start, e.g. if the restriction is {@code
   *     token(...) > :start}, this method will report {@code start}.
   */
  public Optional<CQLWord> getTokenRangeRestrictionStartVariable() {
    return Optional.ofNullable(tokenRangeRestrictionStartVariable);
  }

  /**
   * @return the variable name used to define a token range end, e.g. if the restriction is {@code
   *     token(...) <= :end}, this method will report {@code end}.
   */
  public Optional<CQLWord> getTokenRangeRestrictionEndVariable() {
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

  /**
   * @return Whether this query can be parallelized by splitting the query in many token range
   *     reads. Only applicable for SELECT statements.
   */
  public boolean isParallelizable() {
    return parallelizable;
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
      throw new IllegalArgumentException(
          String.format(
              "Invalid query: the number of columns to insert (%d) does not match the number of terms (%d): %s.",
              ctx.cident().size(), ctx.term().size(), query));
    }
    for (int i = 0; i < ctx.cident().size(); i++) {
      CQLWord column = visitCident(ctx.cident().get(i));
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
    throw new IllegalArgumentException(
        String.format("Invalid query: INSERT JSON is not supported: %s.", query));
  }

  // UPDATE

  @Override
  public CQLFragment visitUpdateStatement(UpdateStatementContext ctx) {
    visitColumnFamilyName(ctx.columnFamilyName());
    for (ColumnOperationContext op : ctx.columnOperation()) {
      CQLWord column = visitCident(op.cident());
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
      throw new IllegalArgumentException(
          String.format("Invalid query: SELECT JSON is not supported: %s.", query));
    }
    visitColumnFamilyName(ctx.columnFamilyName());
    fromClauseStartIndex = ctx.K_FROM().getSymbol().getStartIndex();
    fromClauseEndIndex = ctx.columnFamilyName().getStop().getStopIndex();
    if (ctx.whereClause() != null) {
      hasWhereClause = true;
      parallelizable = false;
      visitWhereClause(ctx.whereClause());
    }
    if (!ctx.groupByClause().isEmpty()
        || !ctx.orderByClause().isEmpty()
        || ctx.limitClause() != null) {
      parallelizable = false;
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
      CQLWord keyspaceName = null;
      if (ctx.functionName().keyspaceName() != null) {
        keyspaceName = visitKeyspaceName(ctx.functionName().keyspaceName());
      }
      CQLWord functionName = visitAllowedFunctionName(ctx.functionName().allowedFunctionName());
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
      CQLWord column = visitCident(ctx.cident());
      if (column.equals(SOLR_QUERY)) {
        hasSearchClause = true;
      }
      CQLFragment variable = visitTerm(ctx.term().get(0));
      assignmentsBuilder.put(column, variable.equals(QUESTION_MARK) ? column : variable);
    } else if (ctx.K_TOKEN() != null) {
      CQLFragment variable = visitTerm(ctx.term().get(0));
      if (variable instanceof CQLWord) {
        if (variable == QUESTION_MARK) {
          variable = INTERNAL_TOKEN_VARNAME;
        }
        if (ctx.relationType().getText().equals(">")) {
          tokenRangeRestrictionStartVariable = (CQLWord) variable;
          tokenRangeRestrictionStartVariableIndex = tokenRangeRestrictionVariableIndex++;
        } else if (ctx.relationType().getText().equals("<=")) {
          tokenRangeRestrictionEndVariable = (CQLWord) variable;
          tokenRangeRestrictionEndVariableIndex = tokenRangeRestrictionVariableIndex++;
        }
      }
    }
    // other relation types: unsupported
    return null;
  }

  // TERMS AND VALUES

  @Override
  @NonNull
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
  @NonNull
  public FunctionCall visitFunction(FunctionContext ctx) {
    CQLWord keyspaceName = null;
    if (ctx.functionName().keyspaceName() != null) {
      keyspaceName = visitKeyspaceName(ctx.functionName().keyspaceName());
    }
    CQLWord functionName = visitAllowedFunctionName(ctx.functionName().allowedFunctionName());
    List<CQLFragment> args = new ArrayList<>();
    if (ctx.functionArgs() != null) {
      for (TermContext arg : ctx.functionArgs().term()) {
        CQLFragment term = visitTerm(arg);
        if (term == QUESTION_MARK) {
          throw new IllegalArgumentException(
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
  @NonNull
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
  @NonNull
  public CQLFragment visitColumnFamilyName(ColumnFamilyNameContext ctx) {
    if (ctx.ksName() != null) {
      keyspaceName = visitKsName(ctx.ksName());
    }
    tableName = visitCfName(ctx.cfName());
    return tableName;
  }

  @Override
  @NonNull
  public CQLWord visitAllowedFunctionName(AllowedFunctionNameContext ctx) {
    if (ctx.QUOTED_NAME() != null) {
      return CQLWord.fromCql(ctx.getText());
    } else {
      return CQLWord.fromInternal(ctx.getText().toLowerCase());
    }
  }

  @Override
  public CQLWord visitKeyspaceName(KeyspaceNameContext ctx) {
    return visitKsName(ctx.ksName());
  }

  @Override
  @NonNull
  public CQLWord visitKsName(KsNameContext ctx) {
    if (ctx.QUOTED_NAME() != null) {
      return CQLWord.fromCql(ctx.getText());
    } else {
      return CQLWord.fromInternal(ctx.getText().toLowerCase());
    }
  }

  @Override
  @NonNull
  public CQLWord visitCfName(CfNameContext ctx) {
    if (ctx.QUOTED_NAME() != null) {
      return CQLWord.fromCql(ctx.getText());
    } else {
      return CQLWord.fromInternal(ctx.getText().toLowerCase());
    }
  }

  @Override
  @NonNull
  public CQLWord visitCident(CidentContext ctx) {
    if (ctx.QUOTED_NAME() != null) {
      return CQLWord.fromCql(ctx.getText());
    } else {
      return CQLWord.fromInternal(ctx.getText().toLowerCase());
    }
  }

  @Override
  @NonNull
  public CQLWord visitNoncolIdent(NoncolIdentContext ctx) {
    if (ctx.QUOTED_NAME() != null) {
      return CQLWord.fromCql(ctx.getText());
    } else {
      return CQLWord.fromInternal(ctx.getText().toLowerCase());
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
  private CQLWord visitUsingTimestamp(IntValueContext intValueContext) {
    if (intValueContext.noncolIdent() != null) {
      CQLWord variable = visitNoncolIdent(intValueContext.noncolIdent());
      writeTimeVariablesBuilder.add(variable);
      usingTimestampVariable = variable;
    } else if (intValueContext.QMARK() != null) {
      writeTimeVariablesBuilder.add(INTERNAL_TIMESTAMP_VARNAME);
      usingTimestampVariable = INTERNAL_TIMESTAMP_VARNAME;
    }
    return usingTimestampVariable;
  }

  @Nullable
  private CQLWord visitUsingTTL(IntValueContext intValueContext) {
    if (intValueContext.noncolIdent() != null) {
      usingTTLVariable = visitNoncolIdent(intValueContext.noncolIdent());
    } else if (intValueContext.QMARK() != null) {
      usingTTLVariable = INTERNAL_TTL_VARNAME;
    }
    return usingTTLVariable;
  }
}
