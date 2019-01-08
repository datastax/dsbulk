/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.dsbulk.engine.WorkflowType.LOAD;
import static com.datastax.dsbulk.engine.internal.schema.MappingPreference.INDEXED_ONLY;
import static com.datastax.dsbulk.engine.internal.schema.MappingPreference.MAPPED_ONLY;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.internal.utils.StringUtils;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.schema.generated.MappingBaseVisitor;
import com.datastax.dsbulk.engine.schema.generated.MappingLexer;
import com.datastax.dsbulk.engine.schema.generated.MappingParser;
import com.datastax.dsbulk.engine.schema.generated.MappingParser.FunctionArgContext;
import com.datastax.dsbulk.engine.schema.generated.MappingParser.FunctionContext;
import com.datastax.dsbulk.engine.schema.generated.MappingParser.FunctionNameContext;
import com.datastax.dsbulk.engine.schema.generated.MappingParser.IndexContext;
import com.datastax.dsbulk.engine.schema.generated.MappingParser.IndexOrFunctionContext;
import com.datastax.dsbulk.engine.schema.generated.MappingParser.SelectorFunctionArgContext;
import com.datastax.dsbulk.engine.schema.generated.MappingParser.SelectorFunctionContext;
import com.datastax.dsbulk.engine.schema.generated.MappingParser.SimpleEntryContext;
import com.datastax.dsbulk.engine.schema.generated.MappingParser.VariableContext;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class MappingInspector extends MappingBaseVisitor<MappingToken> {

  // A mapping spec may refer to these special variables which are used to bind
  // input fields to the write timestamp or ttl of the record.

  private static final String EXTERNAL_TTL_VARNAME = "__ttl";
  private static final String EXTERNAL_TIMESTAMP_VARNAME = "__timestamp";

  private static final CQLIdentifier WRITETIME = CQLIdentifier.fromInternal("writetime");

  private final MappingPreference mappingPreference;
  private final WorkflowType workflowType;
  private final ProtocolVersion protocolVersion;
  private final CQLIdentifier usingTimestampVariable;
  private final CQLIdentifier usingTTLVariable;

  private final Set<CQLFragment> writeTimeVariablesBuilder = new LinkedHashSet<>();
  private final ImmutableSet<CQLFragment> writeTimeVariables;

  private final LinkedHashMultimap<MappingField, CQLFragment> explicitVariablesBuilder;
  private final ImmutableMultimap<MappingField, CQLFragment> explicitVariables;
  private final List<CQLFragment> excludedVariables;

  private int currentIndex;
  private boolean inferring;
  private boolean indexed;
  private boolean hasRegularMappedEntry = false;
  private boolean hasUsingTimestamp = false;
  private boolean hasUsingTTL = false;

  public MappingInspector(
      @NotNull String mapping,
      @NotNull WorkflowType workflowType,
      @NotNull MappingPreference mappingPreference,
      @NotNull ProtocolVersion protocolVersion,
      @Nullable CQLIdentifier usingTimestampVariable,
      @Nullable CQLIdentifier usingTTLVariable) {
    this.workflowType = workflowType;
    this.mappingPreference = mappingPreference;
    this.protocolVersion = protocolVersion;
    this.usingTimestampVariable = usingTimestampVariable;
    this.usingTTLVariable = usingTTLVariable;
    CodePointCharStream input = CharStreams.fromString(mapping);
    MappingLexer lexer = new MappingLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    MappingParser parser = new MappingParser(tokens);
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
                    "Invalid schema.mapping: mapping could not be parsed at line %d:%d: %s",
                    line, col, msg),
                e);
          }
        };
    lexer.removeErrorListeners();
    lexer.addErrorListener(listener);
    parser.removeErrorListeners();
    parser.addErrorListener(listener);
    MappingParser.MappingContext ctx = parser.mapping();
    explicitVariablesBuilder = LinkedHashMultimap.create();
    excludedVariables = new ArrayList<>();
    visit(ctx);
    if (indexed) {
      // if the mapping is indexed, sort the entries by ascending order of indices for a nicer
      // output.
      explicitVariables = ImmutableMultimap.copyOf(sortFieldsByIndex(explicitVariablesBuilder));
    } else {
      explicitVariables = ImmutableMultimap.copyOf(explicitVariablesBuilder);
    }
    checkDuplicates();
    writeTimeVariables = ImmutableSet.copyOf(writeTimeVariablesBuilder);
  }

  /**
   * @return a map from field names to variable names containing the explicit (i.e., non-inferred)
   *     variables found in the mapping.
   */
  public ImmutableMultimap<MappingField, CQLFragment> getExplicitVariables() {
    return explicitVariables;
  }

  /** @return true if the mapping contains an inferred entry (such as "*=*"), false otherwise. */
  public boolean isInferring() {
    return inferring;
  }

  /**
   * @return the list of variables to exclude from the inferred mapping, as in "* = -c1". Returns
   *     empty if there is no such variable of if the mapping does not contain an inferred entry.
   */
  public List<CQLFragment> getExcludedVariables() {
    return excludedVariables;
  }

  /**
   * @return the variable names found in the query in a USING TIMESTAMP clause, or in the SELECT
   *     clause where the selector is a WRITETIME function call, or empty if none was found.
   */
  public ImmutableSet<CQLFragment> getWriteTimeVariables() {
    return writeTimeVariables;
  }

  /**
   * @return true if the mapping has a field mapped to a USING TIMESTAMP clause (using the special
   *     __timestamp variable or the internal writetime variable name provided at instantiation);
   *     false otherwise.
   */
  public boolean hasUsingTimestamp() {
    return hasUsingTimestamp;
  }

  /**
   * @return true if the mapping has a field mapped to a USING TTL clause (using the special __ttl
   *     variable or the internal TTL variable name provided at instantiation); false otherwise.
   */
  public boolean hasUsingTTL() {
    return hasUsingTTL;
  }

  @Override
  public MappingToken visitMapping(MappingParser.MappingContext ctx) {
    currentIndex = 0;
    if (!ctx.simpleEntry().isEmpty()) {
      indexed = mappingPreference == INDEXED_ONLY;
      for (SimpleEntryContext entry : ctx.simpleEntry()) {
        visitSimpleEntry(entry);
      }
    } else if (!ctx.indexedEntry().isEmpty()) {
      indexed = mappingPreference != MAPPED_ONLY;
      for (MappingParser.IndexedEntryContext entry : ctx.indexedEntry()) {
        visitIndexedEntry(entry);
      }
    } else if (!ctx.mappedEntry().isEmpty()) {
      indexed = false;
      for (MappingParser.MappedEntryContext entry : ctx.mappedEntry()) {
        visitMappedEntry(entry);
      }
      if (hasRegularMappedEntry && mappingPreference == INDEXED_ONLY) {
        throw new BulkConfigurationException(
            "Schema mapping contains named fields, but connector only supports indexed fields, "
                + "please enable support for named fields in the connector, or alternatively, "
                + "provide an indexed mapping of the form: '0=col1,1=col2,...'");
      }
    }
    return null;
  }

  @Override
  public MappingToken visitSimpleEntry(SimpleEntryContext ctx) {
    CQLFragment variable = visitVariableOrFunction(ctx.variableOrFunction());
    if (workflowType == LOAD && isFunctionCall(variable)) {
      throw new BulkConfigurationException(
          "Invalid schema.mapping: simple entries cannot contain function calls when loading, "
              + "please use mapped entries instead.");
    }
    if (mappingPreference == INDEXED_ONLY) {
      explicitVariablesBuilder.put(new IndexedMappingField(currentIndex++), variable);
    } else {
      explicitVariablesBuilder.put(new MappedMappingField(variable.asInternal()), variable);
    }
    return null;
  }

  @Override
  public MappingToken visitIndexedEntry(MappingParser.IndexedEntryContext ctx) {
    MappingField field = visitIndexOrFunction(ctx.indexOrFunction());
    CQLFragment variable = visitVariableOrFunction(ctx.variableOrFunction());
    explicitVariablesBuilder.put(field, variable);
    return null;
  }

  @Override
  public MappingField visitIndexOrFunction(IndexOrFunctionContext ctx) {
    if (ctx.index() != null) {
      return visitIndex(ctx.index());
    } else {
      return visitFunction(ctx.function());
    }
  }

  @Override
  public MappingField visitIndex(IndexContext ctx) {
    String index = ctx.INTEGER().getText();
    if (mappingPreference == MAPPED_ONLY) {
      return new MappedMappingField(index);
    } else {
      return new IndexedMappingField(Integer.parseInt(index));
    }
  }

  @Override
  public MappingToken visitRegularMappedEntry(MappingParser.RegularMappedEntryContext ctx) {
    hasRegularMappedEntry = true;
    MappingField field = visitFieldOrFunction(ctx.fieldOrFunction());
    CQLFragment variable = visitVariableOrFunction(ctx.variableOrFunction());
    explicitVariablesBuilder.put(field, variable);
    return null;
  }

  @Override
  public MappingToken visitInferredMappedEntry(MappingParser.InferredMappedEntryContext ctx) {
    checkInferring();
    inferring = true;
    for (VariableContext variableContext : ctx.variable()) {
      CQLIdentifier variable = visitVariable(variableContext);
      excludedVariables.add(variable);
    }
    return null;
  }

  @Override
  @NotNull
  public MappingField visitFieldOrFunction(MappingParser.FieldOrFunctionContext ctx) {
    if (ctx.field() != null) {
      return visitField(ctx.field());
    } else {
      return visitFunction(ctx.function());
    }
  }

  @Override
  @NotNull
  public MappedMappingField visitField(MappingParser.FieldContext ctx) {
    MappedMappingField field;
    if (ctx.QUOTED_IDENTIFIER() != null) {
      field = new MappedMappingField(StringUtils.unDoubleQuote(ctx.QUOTED_IDENTIFIER().getText()));
    } else {
      field = new MappedMappingField(ctx.UNQUOTED_IDENTIFIER().getText());
    }
    return field;
  }

  @Override
  @NotNull
  public CQLFragment visitVariableOrFunction(MappingParser.VariableOrFunctionContext ctx) {
    if (ctx.variable() != null) {
      return visitVariable(ctx.variable());
    } else {
      return visitSelectorFunction(ctx.selectorFunction());
    }
  }

  @Override
  @NotNull
  public CQLIdentifier visitVariable(VariableContext ctx) {
    CQLIdentifier variable;
    if (ctx.QUOTED_IDENTIFIER() != null) {
      variable = CQLIdentifier.fromCql(ctx.QUOTED_IDENTIFIER().getText());
    } else {
      String text = ctx.UNQUOTED_IDENTIFIER().getText();
      // Rename the user-specified __ttl and __timestamp vars to the (legal) bound variable
      // names provided at instantiation.
      if (text.equals(EXTERNAL_TTL_VARNAME)) {
        if (usingTTLVariable == null) {
          throw new BulkConfigurationException(
              String.format(
                  "Invalid mapping: %s variable is not allowed when schema.query does not contain a USING TTL clause",
                  EXTERNAL_TTL_VARNAME));
        }
        variable = usingTTLVariable;
      } else if (text.equals(EXTERNAL_TIMESTAMP_VARNAME)) {
        if (usingTimestampVariable == null) {
          throw new BulkConfigurationException(
              String.format(
                  "Invalid mapping: %s variable is not allowed when schema.query does not contain a USING TIMESTAMP clause",
                  EXTERNAL_TIMESTAMP_VARNAME));
        }
        variable = usingTimestampVariable;
      } else {
        // Note: contrary to how the CQL grammar handles unquoted identifiers,
        // in a mapping entry we do not lower-case the unquoted identifier,
        // to avoid forcing users to quote identifiers just because they are case-sensitive.
        variable = CQLIdentifier.fromInternal(text);
      }
    }
    if (variable.equals(usingTimestampVariable)) {
      writeTimeVariablesBuilder.add(variable);
      hasUsingTimestamp = true;
    }
    if (variable.equals(usingTTLVariable)) {
      hasUsingTTL = true;
    }
    return variable;
  }

  @Override
  @NotNull
  public FunctionCall visitFunction(FunctionContext ctx) {
    List<CQLFragment> args = new ArrayList<>();
    if (ctx.functionArgs() != null) {
      for (FunctionArgContext arg : ctx.functionArgs().functionArg()) {
        args.add(visitFunctionArg(arg));
      }
    }
    return new FunctionCall(visitFunctionName(ctx.functionName()), args);
  }

  @Override
  @NotNull
  public CQLFragment visitSelectorFunction(SelectorFunctionContext ctx) {
    CQLFragment functionCall;
    if (ctx.WRITETIME() != null) {
      functionCall =
          maybeCreateAlias(
              new FunctionCall(
                  WRITETIME,
                  Collections.singletonList(visitSelectorFunctionArg(ctx.selectorFunctionArg()))));
      writeTimeVariablesBuilder.add(functionCall);
    } else {
      List<CQLFragment> args = new ArrayList<>();
      if (ctx.selectorFunctionArgs() != null) {
        for (SelectorFunctionArgContext arg : ctx.selectorFunctionArgs().selectorFunctionArg()) {
          args.add(visitSelectorFunctionArg(arg));
        }
      }
      functionCall =
          maybeCreateAlias(new FunctionCall(visitFunctionName(ctx.functionName()), args));
    }
    return functionCall;
  }

  @Override
  @NotNull
  public CQLIdentifier visitFunctionName(FunctionNameContext ctx) {
    CQLIdentifier functionName;
    if (ctx.QUOTED_IDENTIFIER() != null) {
      functionName = CQLIdentifier.fromCql(ctx.QUOTED_IDENTIFIER().getText());
    } else {
      functionName = CQLIdentifier.fromCql(ctx.UNQUOTED_IDENTIFIER().getText());
    }
    return functionName;
  }

  @Override
  @NotNull
  public CQLLiteral visitFunctionArg(FunctionArgContext ctx) {
    return new CQLLiteral(ctx.getText());
  }

  @Override
  @NotNull
  public CQLFragment visitSelectorFunctionArg(SelectorFunctionArgContext ctx) {
    CQLFragment functionArg;
    if (ctx.QUOTED_IDENTIFIER() != null) {
      functionArg = CQLIdentifier.fromCql(ctx.QUOTED_IDENTIFIER().getText());
    } else if (ctx.UNQUOTED_IDENTIFIER() != null) {
      functionArg = CQLIdentifier.fromCql(ctx.UNQUOTED_IDENTIFIER().getText());
    } else {
      functionArg = visitFunctionArg(ctx.functionArg());
    }
    return functionArg;
  }

  private void checkInferring() {
    if (inferring) {
      throw new BulkConfigurationException(
          "Invalid schema.mapping: inferred mapping entry (* = *) can be supplied at most once.");
    }
  }

  public static LinkedHashMultimap<MappingField, CQLFragment> sortFieldsByIndex(
      Multimap<MappingField, CQLFragment> unsorted) {
    LinkedHashMultimap<MappingField, CQLFragment> sorted = LinkedHashMultimap.create();
    unsorted
        .entries()
        .stream()
        .sorted(
            Entry.comparingByKey(Comparator.comparingInt(MappingInspector::compareIndexedFields)))
        .forEachOrdered(entry -> sorted.put(entry.getKey(), entry.getValue()));
    return sorted;
  }

  private static int compareIndexedFields(MappingField field) {
    if (field instanceof IndexedMappingField) {
      return ((IndexedMappingField) field).getFieldIndex();
    } else {
      assert field instanceof FunctionCall;
      // functions go after, they will be removed from the mapping anyway
      return Integer.MIN_VALUE;
    }
  }

  private void checkDuplicates() {
    Collection<? extends MappingToken> toCheck;
    if (workflowType == LOAD) {
      // OK: field1 = col1, field1 = col2
      // KO: field1 = col1, field2 = col1
      toCheck = explicitVariables.values();
    } else {
      // OK: field1 = col1, field2 = col1
      // KO: field1 = col1, field1 = col2
      toCheck = explicitVariables.inverse().values();
    }
    List<MappingToken> duplicates =
        toCheck
            .stream()
            .collect(groupingBy(v -> v, counting()))
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue() > 1)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    if (!duplicates.isEmpty()) {
      String msg;
      if (workflowType == LOAD) {
        msg = "the following variables are mapped to more than one field";
      } else {
        msg = "the following fields are mapped to more than one variable";
      }
      String offending =
          duplicates.stream().map(Object::toString).collect(Collectors.joining(", "));
      throw new BulkConfigurationException(
          String.format(
              "Invalid schema.mapping: %s: %s. Please review schema.mapping for duplicates.",
              msg, offending));
    }
  }

  private CQLFragment maybeCreateAlias(CQLFragment target) {
    if (protocolVersion == ProtocolVersion.V1) {
      return target;
    }
    return new Alias(target, CQLIdentifier.fromInternal(target.asInternal()));
  }

  private static boolean isFunctionCall(CQLFragment variable) {
    if (variable instanceof Alias) {
      variable = ((Alias) variable).getTarget();
    }
    return variable instanceof FunctionCall;
  }
}
