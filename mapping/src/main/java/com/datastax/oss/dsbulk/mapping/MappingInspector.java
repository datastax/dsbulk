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
package com.datastax.oss.dsbulk.mapping;

import static com.datastax.oss.dsbulk.mapping.CQLRenderMode.INTERNAL;
import static com.datastax.oss.dsbulk.mapping.CQLRenderMode.VARIABLE;
import static com.datastax.oss.dsbulk.mapping.MappingPreference.INDEXED_ONLY;
import static com.datastax.oss.dsbulk.mapping.MappingPreference.MAPPED_ONLY;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.LinkedHashMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.Multimap;
import com.datastax.oss.dsbulk.generated.mapping.MappingBaseVisitor;
import com.datastax.oss.dsbulk.generated.mapping.MappingLexer;
import com.datastax.oss.dsbulk.generated.mapping.MappingParser;
import com.datastax.oss.dsbulk.generated.mapping.MappingParser.ColumnNameContext;
import com.datastax.oss.dsbulk.generated.mapping.MappingParser.FieldContext;
import com.datastax.oss.dsbulk.generated.mapping.MappingParser.FunctionArgContext;
import com.datastax.oss.dsbulk.generated.mapping.MappingParser.FunctionContext;
import com.datastax.oss.dsbulk.generated.mapping.MappingParser.FunctionNameContext;
import com.datastax.oss.dsbulk.generated.mapping.MappingParser.IdentifierContext;
import com.datastax.oss.dsbulk.generated.mapping.MappingParser.IndexContext;
import com.datastax.oss.dsbulk.generated.mapping.MappingParser.IndexOrFunctionContext;
import com.datastax.oss.dsbulk.generated.mapping.MappingParser.KeyspaceNameContext;
import com.datastax.oss.dsbulk.generated.mapping.MappingParser.LiteralContext;
import com.datastax.oss.dsbulk.generated.mapping.MappingParser.QualifiedFunctionNameContext;
import com.datastax.oss.dsbulk.generated.mapping.MappingParser.SimpleEntryContext;
import com.datastax.oss.dsbulk.generated.mapping.MappingParser.TtlContext;
import com.datastax.oss.dsbulk.generated.mapping.MappingParser.VariableContext;
import com.datastax.oss.dsbulk.generated.mapping.MappingParser.VariableOrFunctionContext;
import com.datastax.oss.dsbulk.generated.mapping.MappingParser.WritetimeContext;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MappingInspector extends MappingBaseVisitor<MappingToken> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MappingInspector.class);

  private static final String EXTERNAL_TTL_VARNAME = "__ttl";
  private static final String EXTERNAL_TIMESTAMP_VARNAME = "__timestamp";

  public static final CQLWord WRITETIME = CQLWord.fromInternal("writetime");
  public static final CQLWord TTL = CQLWord.fromInternal("ttl");
  public static final CQLWord STAR = CQLWord.fromInternal("*");

  private final MappingPreference mappingPreference;
  private final boolean write;

  private final Supplier<CQLWord> usingTimestampVariable;
  private final Supplier<CQLWord> usingTTLVariable;

  private final LinkedHashMultimap<MappingField, CQLFragment> explicitMappingsBuilder;
  private final ImmutableMultimap<MappingField, CQLFragment> explicitMappings;
  private final List<CQLWord> excludedVariables;

  private int currentIndex;
  private boolean inferring;
  private boolean indexed;
  private boolean hasRegularMappedEntry = false;

  public MappingInspector(
      @NonNull String mapping, boolean write, @NonNull MappingPreference mappingPreference) {
    this(mapping, write, mappingPreference, null, null);
  }

  /**
   * @deprecated As of DSBulk 1.8.0, the mapping inspector deprecates the special __ttl and
   *     __timestamp tokens; these will be removed in a future release. Therefore, providing
   *     external names for these tokens will also cease to be supported in a future release.
   * @param usingTimestampVariable The name of the USING TIMESTAMP variable, or empty if no such
   *     variable, or null if no query at all.
   * @param usingTTLVariable The name of the USING TTL variable, or empty if no such variable, or
   *     null if no query at all.
   */
  @Deprecated
  @SuppressWarnings("DeprecatedIsStillUsed")
  public MappingInspector(
      @NonNull String mapping,
      boolean write,
      @NonNull MappingPreference mappingPreference,
      @Nullable Supplier<CQLWord> usingTimestampVariable,
      @Nullable Supplier<CQLWord> usingTTLVariable) {
    this.write = write;
    this.mappingPreference = mappingPreference;
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
            throw new IllegalArgumentException(
                String.format(
                    "Invalid mapping: mapping could not be parsed at line %d:%d: %s",
                    line, col, msg),
                e);
          }
        };
    lexer.removeErrorListeners();
    lexer.addErrorListener(listener);
    parser.removeErrorListeners();
    parser.addErrorListener(listener);
    MappingParser.MappingContext ctx = parser.mapping();
    explicitMappingsBuilder = LinkedHashMultimap.create();
    excludedVariables = new ArrayList<>();
    visit(ctx);
    if (indexed) {
      // if the mapping is indexed, sort the entries by ascending order of indices for a nicer
      // output.
      explicitMappings = ImmutableMultimap.copyOf(sortFieldsByIndex(explicitMappingsBuilder));
    } else {
      explicitMappings = ImmutableMultimap.copyOf(explicitMappingsBuilder);
    }
    checkDuplicates();
  }

  /**
   * @return a map from field names to variable names containing the explicit (i.e., non-inferred)
   *     variables found in the mapping.
   */
  public ImmutableMultimap<MappingField, CQLFragment> getExplicitMappings() {
    return explicitMappings;
  }

  /** @return true if the mapping contains an inferred entry (such as "*=*"), false otherwise. */
  public boolean isInferring() {
    return inferring;
  }

  /**
   * @return the list of variables to exclude from the inferred mapping, as in "* = -c1". Returns
   *     empty if there is no such variable of if the mapping does not contain an inferred entry.
   */
  public List<CQLWord> getExcludedVariables() {
    return excludedVariables;
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
        throw new IllegalArgumentException(
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
    if (write && variable instanceof FunctionCall) {
      throw new IllegalArgumentException(
          "Invalid mapping: simple entries cannot contain function calls when loading, "
              + "please use mapped entries instead.");
    }
    if (mappingPreference == INDEXED_ONLY) {
      explicitMappingsBuilder.put(new IndexedMappingField(currentIndex++), variable);
    } else {
      String fieldName = variable.render(INTERNAL);
      explicitMappingsBuilder.put(new MappedMappingField(fieldName), variable);
    }
    return null;
  }

  @Override
  public MappingToken visitRegularMappedEntry(MappingParser.RegularMappedEntryContext ctx) {
    hasRegularMappedEntry = true;
    MappingField field = visitFieldOrFunction(ctx.fieldOrFunction());
    CQLFragment variable = visitVariableOrFunction(ctx.variableOrFunction());
    explicitMappingsBuilder.put(field, variable);
    return null;
  }

  @Override
  public MappingToken visitIndexedEntry(MappingParser.IndexedEntryContext ctx) {
    MappingField field = visitIndexOrFunction(ctx.indexOrFunction());
    CQLFragment variable = visitVariableOrFunction(ctx.variableOrFunction());
    explicitMappingsBuilder.put(field, variable);
    return null;
  }

  @Override
  public MappingToken visitInferredMappedEntry(MappingParser.InferredMappedEntryContext ctx) {
    checkInferring();
    inferring = true;
    for (VariableContext variableContext : ctx.variable()) {
      CQLWord variable = visitVariable(variableContext);
      excludedVariables.add(variable);
    }
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
  @NonNull
  public MappingField visitFieldOrFunction(MappingParser.FieldOrFunctionContext ctx) {
    if (ctx.field() != null) {
      return visitField(ctx.field());
    } else {
      return visitFunction(ctx.function());
    }
  }

  @Override
  @NonNull
  public CQLFragment visitVariableOrFunction(MappingParser.VariableOrFunctionContext ctx) {
    if (ctx.variable() != null) {
      if (ctx.variable().getText().equals(EXTERNAL_TTL_VARNAME)) {
        if (!write) {
          throw new IllegalArgumentException(
              String.format(
                  "Invalid mapping: %s mapping token not allowed when unloading.",
                  EXTERNAL_TTL_VARNAME));
        }
        if (usingTTLVariable == null) {
          LOGGER.warn(
              "The special {} mapping token has been deprecated; "
                  + "please use a ttl(*) function call instead.",
              EXTERNAL_TTL_VARNAME);
          return new FunctionCall(null, TTL, Collections.singletonList(STAR));
        } else {
          CQLWord variable = usingTTLVariable.get();
          if (variable == null) {
            throw new IllegalArgumentException(
                String.format(
                    "Invalid mapping: %s mapping token is not allowed when the companion query does not contain a USING TTL clause",
                    EXTERNAL_TTL_VARNAME));
          } else {
            LOGGER.warn(
                "The special {} mapping token has been deprecated; "
                    + "since a companion query is provided, please use instead "
                    + "the bound variable name assigned to the USING TTL clause: {}.",
                EXTERNAL_TTL_VARNAME,
                variable.render(VARIABLE));
          }
          return variable;
        }
      } else if (ctx.variable().getText().equals(EXTERNAL_TIMESTAMP_VARNAME)) {
        if (!write) {
          throw new IllegalArgumentException(
              String.format(
                  "Invalid mapping: %s mapping token not allowed when unloading.",
                  EXTERNAL_TIMESTAMP_VARNAME));
        }
        if (usingTimestampVariable == null) {
          LOGGER.warn(
              "The special {} mapping token has been deprecated; "
                  + "please use a writetime(*) function call instead.",
              EXTERNAL_TIMESTAMP_VARNAME);
          return new FunctionCall(null, WRITETIME, Collections.singletonList(STAR));
        } else {
          CQLWord variable = usingTimestampVariable.get();
          if (variable == null) {
            throw new IllegalArgumentException(
                String.format(
                    "Invalid mapping: %s mapping token is not allowed when the companion query does not contain a USING TIMESTAMP clause",
                    EXTERNAL_TIMESTAMP_VARNAME));
          } else {
            LOGGER.warn(
                "The special {} mapping token has been deprecated; "
                    + "since a companion query is provided, please use instead "
                    + "the bound variable name assigned to the USING TIMESTAMP clause: {}.",
                EXTERNAL_TIMESTAMP_VARNAME,
                variable.render(VARIABLE));
          }
          return variable;
        }
      }
      return visitVariable(ctx.variable());
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
  @NonNull
  public MappedMappingField visitField(FieldContext ctx) {
    CQLWord identifier = visitIdentifier(ctx.identifier());
    return new MappedMappingField(identifier.render(INTERNAL));
  }

  @Override
  @NonNull
  public CQLWord visitVariable(VariableContext ctx) {
    return visitIdentifier(ctx.identifier());
  }

  @Override
  @NonNull
  public FunctionCall visitFunction(FunctionContext ctx) {
    if (ctx.writetime() != null) {
      return visitWritetime(ctx.writetime());
    } else if (ctx.ttl() != null) {
      return visitTtl(ctx.ttl());
    } else {
      CQLWord keyspaceName = null;
      QualifiedFunctionNameContext name = ctx.qualifiedFunctionName();
      if (name.keyspaceName() != null) {
        keyspaceName = visitKeyspaceName(name.keyspaceName());
      }
      CQLWord functionName = visitFunctionName(name.functionName());
      List<CQLFragment> args = new ArrayList<>();
      if (ctx.functionArgs() != null) {
        for (FunctionArgContext arg : ctx.functionArgs().functionArg()) {
          args.add(visitFunctionArg(arg));
        }
      }
      return new FunctionCall(keyspaceName, functionName, args);
    }
  }

  @Override
  public FunctionCall visitWritetime(WritetimeContext ctx) {
    if (!(ctx.getParent().getParent() instanceof VariableOrFunctionContext)) {
      throw new IllegalArgumentException(
          "Invalid mapping: writetime() function calls not allowed on the left side of a mapping entry.");
    }
    List<CQLFragment> args = new ArrayList<>();
    if (ctx.STAR() != null) {
      if (!write) {
        throw new IllegalArgumentException(
            "Invalid mapping: writetime(*) function calls not allowed when unloading.");
      }
      args = Collections.singletonList(STAR);
    } else {
      assert ctx.columnName() != null;
      if (!write && ctx.columnName().size() > 1) {
        throw new IllegalArgumentException(
            "Invalid mapping: writetime() function calls must have exactly one argument when unloading.");
      }
      for (ColumnNameContext col : ctx.columnName()) {
        args.add(visitColumnName(col));
      }
    }
    return new FunctionCall(null, WRITETIME, args);
  }

  @Override
  public FunctionCall visitTtl(TtlContext ctx) {
    if (!(ctx.getParent().getParent() instanceof VariableOrFunctionContext)) {
      throw new IllegalArgumentException(
          "Invalid mapping: ttl() function calls not allowed on the left side of a mapping entry.");
    }
    List<CQLFragment> args = new ArrayList<>();
    if (ctx.STAR() != null) {
      if (!write) {
        throw new IllegalArgumentException(
            "Invalid mapping: ttl(*) function calls not allowed when unloading.");
      }
      args = Collections.singletonList(STAR);
    } else {
      assert ctx.columnName() != null;
      if (!write && ctx.columnName().size() > 1) {
        throw new IllegalArgumentException(
            "Invalid mapping: ttl() function calls must have exactly one argument when unloading.");
      }
      for (ColumnNameContext col : ctx.columnName()) {
        args.add(visitColumnName(col));
      }
    }
    return new FunctionCall(null, TTL, args);
  }

  @Override
  public CQLWord visitKeyspaceName(KeyspaceNameContext ctx) {
    return visitIdentifier(ctx.identifier());
  }

  @Override
  @NonNull
  public CQLWord visitFunctionName(FunctionNameContext ctx) {
    return visitIdentifier(ctx.identifier());
  }

  @Override
  public CQLWord visitColumnName(ColumnNameContext ctx) {
    return visitIdentifier(ctx.identifier());
  }

  @Override
  public CQLWord visitIdentifier(IdentifierContext ctx) {
    if (ctx.QUOTED_IDENTIFIER() != null) {
      return CQLWord.fromCql(ctx.QUOTED_IDENTIFIER().getText());
    } else if (ctx.WRITETIME() != null) {
      return WRITETIME;
    } else if (ctx.TTL() != null) {
      return TTL;
    } else {
      assert ctx.UNQUOTED_IDENTIFIER() != null;
      // Note: contrary to how the CQL grammar handles unquoted identifiers,
      // in a mapping entry we do not lower-case the unquoted identifier,
      // to avoid forcing users to quote identifiers just because they are case-sensitive.
      return CQLWord.fromInternal(ctx.UNQUOTED_IDENTIFIER().getText());
    }
  }

  @Override
  @NonNull
  public CQLFragment visitFunctionArg(FunctionArgContext ctx) {
    if (ctx.columnName() != null) {
      return visitColumnName(ctx.columnName());
    } else if (ctx.literal() != null) {
      return visitLiteral(ctx.literal());
    } else {
      assert ctx.function() != null;
      return visitFunction(ctx.function());
    }
  }

  @Override
  public CQLLiteral visitLiteral(LiteralContext ctx) {
    return new CQLLiteral(ctx.getText());
  }

  private void checkInferring() {
    if (inferring) {
      throw new IllegalArgumentException(
          "Invalid mapping: inferred mapping entry (* = *) can be supplied at most once.");
    }
  }

  public static LinkedHashMultimap<MappingField, CQLFragment> sortFieldsByIndex(
      Multimap<MappingField, CQLFragment> unsorted) {
    LinkedHashMultimap<MappingField, CQLFragment> sorted = LinkedHashMultimap.create();
    unsorted.entries().stream()
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
      // functions in fields are not included in the final mapping since
      // the generated query will contain the function call as is;
      // their place in the resulting mappings list is thus irrelevant,
      // so put them after the indexed fields.
      return Integer.MAX_VALUE;
    }
  }

  private void checkDuplicates() {
    Collection<? extends MappingToken> toCheck;
    if (write) {
      // OK: field1 = col1, field1 = col2
      // KO: field1 = col1, field2 = col1
      toCheck = explicitMappings.values();
    } else {
      // OK: field1 = col1, field2 = col1
      // KO: field1 = col1, field1 = col2
      toCheck = explicitMappings.inverse().values();
    }
    List<MappingToken> duplicates =
        toCheck.stream()
            .collect(groupingBy(v -> v, counting()))
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue() > 1)
            .map(Entry::getKey)
            .collect(Collectors.toList());
    if (!duplicates.isEmpty()) {
      String msg;
      if (write) {
        msg = "the following variables are mapped to more than one field";
      } else {
        msg = "the following fields are mapped to more than one variable";
      }
      String offending =
          duplicates.stream().map(Object::toString).collect(Collectors.joining(", "));
      throw new IllegalArgumentException(
          String.format(
              "Invalid mapping: %s: %s. Please review mapping entries for duplicates.",
              msg, offending));
    }
  }
}
