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
import com.datastax.dsbulk.engine.schema.MappingBaseVisitor;
import com.datastax.dsbulk.engine.schema.MappingLexer;
import com.datastax.dsbulk.engine.schema.MappingParser;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableBiMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

public class MappingInspector extends MappingBaseVisitor<String> {

  // A mapping spec may refer to these special variables which are used to bind
  // input fields to the write timestamp or ttl of the record.

  public static final String INTERNAL_TTL_VARNAME = "dsbulk_internal_ttl";
  public static final String INTERNAL_TIMESTAMP_VARNAME = "dsbulk_internal_timestamp";

  private static final String EXTERNAL_TTL_VARNAME = "__ttl";
  private static final String EXTERNAL_TIMESTAMP_VARNAME = "__timestamp";

  private static final CharMatcher DIGIT = CharMatcher.inRange('0', '9');

  private final boolean preferIndexedMapping;

  private LinkedHashMap<String, CqlIdentifier> explicitVariables;
  private ImmutableBiMap<String, CqlIdentifier> explicitVariablesBimap;
  private int currentIndex;
  private boolean inferring;
  private List<CqlIdentifier> excludedVariables;

  public MappingInspector(String mapping, boolean preferIndexedMapping) {
    this.preferIndexedMapping = preferIndexedMapping;
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
    visit(ctx);
  }

  public ImmutableBiMap<String, CqlIdentifier> getExplicitVariables() {
    return explicitVariablesBimap;
  }

  public boolean isInferring() {
    return inferring;
  }

  public List<CqlIdentifier> getExcludedVariables() {
    return excludedVariables;
  }

  @Override
  public String visitMapping(MappingParser.MappingContext ctx) {
    explicitVariables = new LinkedHashMap<>();
    excludedVariables = new ArrayList<>();
    currentIndex = 0;
    if (!ctx.indexedEntry().isEmpty()) {
      for (MappingParser.IndexedEntryContext entry : ctx.indexedEntry()) {
        visitIndexedEntry(entry);
      }
    } else if (!ctx.mappedEntry().isEmpty()) {
      for (MappingParser.MappedEntryContext entry : ctx.mappedEntry()) {
        visitMappedEntry(entry);
      }
    }
    // if keys are indices, sort by index
    if (explicitVariables.keySet().stream().allMatch(DIGIT::matchesAllOf)) {
      LinkedHashMap<String, CqlIdentifier> unsorted = explicitVariables;
      explicitVariables = new LinkedHashMap<>();
      unsorted
          .entrySet()
          .stream()
          .sorted(Map.Entry.comparingByKey(Comparator.comparingInt(Integer::parseInt)))
          .forEachOrdered(entry -> explicitVariables.put(entry.getKey(), entry.getValue()));
    }
    checkDuplicates();
    explicitVariablesBimap = ImmutableBiMap.copyOf(explicitVariables);
    return null;
  }

  @Override
  public String visitIndexedEntry(MappingParser.IndexedEntryContext ctx) {
    String variable = visitVariable(ctx.variable());
    if (preferIndexedMapping) {
      explicitVariables.put(Integer.toString(currentIndex++), CqlIdentifier.fromCql(variable));
    } else {
      // TODO: This may argue for making the key of the map a CqlIdentifier...
      // the reason we haven't is that in some cases the "variable" may be a function call and
      // thus should probably not be an id.
      CqlIdentifier variableId = CqlIdentifier.fromCql(variable);
      explicitVariables.put(variableId.asInternal(), variableId);
    }
    return null;
  }

  @Override
  public String visitRegularMappedEntry(MappingParser.RegularMappedEntryContext ctx) {
    String field = visitField(ctx.field());
    String variable = visitVariable(ctx.variable());
    explicitVariables.put(field, CqlIdentifier.fromCql(variable));
    return null;
  }

  @Override
  public String visitInferredMappedEntry(MappingParser.InferredMappedEntryContext ctx) {
    checkInferring();
    inferring = true;
    for (MappingParser.VariableContext variableContext : ctx.variable()) {
      String variable = visitVariable(variableContext);
      excludedVariables.add(CqlIdentifier.fromCql(variable));
    }
    return null;
  }

  @Override
  public String visitField(MappingParser.FieldContext ctx) {
    String field = ctx.getText();
    if (ctx.QUOTED_STRING() != null) {
      field = field.substring(1, field.length() - 1).replace("\"\"", "\"");
    }
    return field;
  }

  @Override
  public String visitVariable(MappingParser.VariableContext ctx) {
    String variable = ctx.getText();
    if (ctx.QUOTED_STRING() == null) {
      // Rename the user-specified __ttl and __timestamp vars to the (legal) bound variable
      // names.
      if (variable.equals(EXTERNAL_TTL_VARNAME)) {
        variable = INTERNAL_TTL_VARNAME;
      } else if (variable.equals(EXTERNAL_TIMESTAMP_VARNAME)) {
        variable = INTERNAL_TIMESTAMP_VARNAME;
      }
    }
    return variable;
  }

  private void checkInferring() {
    if (inferring) {
      throw new BulkConfigurationException(
          "Invalid schema.mapping: inferred mapping entry (* = *) can be supplied at most once.");
    }
  }

  private void checkDuplicates() {
    List<CqlIdentifier> duplicates =
        explicitVariables
            .values()
            .stream()
            .collect(Collectors.groupingBy(v -> v, Collectors.counting()))
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue() > 1)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    if (!duplicates.isEmpty()) {
      throw new BulkConfigurationException(
          "Invalid schema.mapping: the following variables are mapped to more than one field: "
              + duplicates
                  .stream()
                  .map(CqlIdentifier::asInternal)
                  .sorted()
                  .collect(Collectors.joining(", "))
              + ". "
              + "Please review schema.mapping for duplicates.");
    }
  }
}
