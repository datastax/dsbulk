package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.schema.CQLFragment;
import com.datastax.dsbulk.engine.internal.schema.MappingField;

import java.util.Collections;
import java.util.Map;

public class InferredQuery {
  private final String query;
  private final WorkflowType workflowType;
  private final Map<MappingField, CQLFragment> loadFieldToFunction;

  public InferredQuery(String query, WorkflowType workflowType, Map<MappingField, CQLFragment> loadFieldToFunction) {
    this.query = query;
    this.workflowType = workflowType;
    this.loadFieldToFunction = loadFieldToFunction;
  }

  public static InferredQuery count(String query) {
    return new InferredQuery(query, WorkflowType.COUNT, Collections.emptyMap());
  }

  public static InferredQuery load(String query) {
    return new InferredQuery(query, WorkflowType.LOAD, Collections.emptyMap());
  }

  public static InferredQuery unload(String query) {
    return new InferredQuery(query, WorkflowType.UNLOAD, Collections.emptyMap());
  }

  public static InferredQuery load(String query, Map<MappingField, CQLFragment> functions) {
    return new InferredQuery(query, WorkflowType.LOAD, functions);
  }

  public InferredQuery copyWith(String query) {
    return new InferredQuery(query, this.workflowType, this.loadFieldToFunction);
  }

  public String getQuery() {
    return query;
  }

  public WorkflowType getWorkflowType() {
    return workflowType;
  }

  public Map<MappingField, CQLFragment> getLoadFieldToFunction() {
    return loadFieldToFunction;
  }
}
