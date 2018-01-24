/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.config;

import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigMemorySize;
import com.typesafe.config.ConfigMergeable;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigOrigin;
import com.typesafe.config.ConfigResolveOptions;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DefaultLoaderConfig implements LoaderConfig {

  private final Config delegate;

  public DefaultLoaderConfig(Config delegate) {
    this.delegate = delegate;
  }

  @Override
  public ConfigObject root() {
    return delegate.root();
  }

  @Override
  public ConfigOrigin origin() {
    return delegate.origin();
  }

  @Override
  public LoaderConfig withFallback(ConfigMergeable other) {
    if (other instanceof DefaultLoaderConfig) {
      // DefaultLoaderConfig does not implement ConfigMergeable
      other = ((DefaultLoaderConfig) other).delegate;
    }
    return new DefaultLoaderConfig(delegate.withFallback(other));
  }

  @Override
  public LoaderConfig resolve() {
    return new DefaultLoaderConfig(delegate.resolve());
  }

  @Override
  public LoaderConfig resolve(ConfigResolveOptions options) {
    return new DefaultLoaderConfig(delegate.resolve(options));
  }

  @Override
  public boolean isResolved() {
    return delegate.isResolved();
  }

  @Override
  public LoaderConfig resolveWith(Config source) {
    return new DefaultLoaderConfig(delegate.resolveWith(source));
  }

  @Override
  public LoaderConfig resolveWith(Config source, ConfigResolveOptions options) {
    return new DefaultLoaderConfig(delegate.resolveWith(source, options));
  }

  @Override
  public void checkValid(Config reference, String... restrictToPaths) {
    delegate.checkValid(reference, restrictToPaths);
  }

  @Override
  public boolean hasPath(String path) {
    // Since defaults may be empty string (representing null), such paths don't count.
    return (delegate.hasPath(path)
        && (delegate.getValue(path).valueType() != ConfigValueType.STRING
            || !delegate.getString(path).isEmpty()));
  }

  @Override
  public boolean hasPathOrNull(String path) {
    return delegate.hasPathOrNull(path);
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public Set<Map.Entry<String, ConfigValue>> entrySet() {
    return delegate.entrySet();
  }

  @Override
  public boolean getIsNull(String path) {
    return delegate.getIsNull(path);
  }

  @Override
  public boolean getBoolean(String path) {
    return delegate.getBoolean(path);
  }

  @Override
  public Number getNumber(String path) {
    return delegate.getNumber(path);
  }

  @Override
  public int getInt(String path) {
    return delegate.getInt(path);
  }

  @Override
  public long getLong(String path) {
    return delegate.getLong(path);
  }

  @Override
  public double getDouble(String path) {
    return delegate.getDouble(path);
  }

  @Override
  public String getString(String path) {
    return delegate.getString(path);
  }

  @Override
  public <T extends Enum<T>> T getEnum(Class<T> enumClass, String path) {
    return delegate.getEnum(enumClass, path);
  }

  @Override
  public ConfigObject getObject(String path) {
    return delegate.getObject(path);
  }

  @Override
  public LoaderConfig getConfig(String path) {
    return new DefaultLoaderConfig(delegate.getConfig(path));
  }

  @Override
  public Object getAnyRef(String path) {
    return delegate.getAnyRef(path);
  }

  @Override
  public ConfigValue getValue(String path) {
    return delegate.getValue(path);
  }

  @Override
  public Long getBytes(String path) {
    return delegate.getBytes(path);
  }

  @Override
  public ConfigMemorySize getMemorySize(String path) {
    return delegate.getMemorySize(path);
  }

  @Override
  @Deprecated
  public Long getMilliseconds(String path) {
    return delegate.getMilliseconds(path);
  }

  @Override
  @Deprecated
  public Long getNanoseconds(String path) {
    return delegate.getNanoseconds(path);
  }

  @Override
  public long getDuration(String path, TimeUnit unit) {
    return delegate.getDuration(path, unit);
  }

  @Override
  public Duration getDuration(String path) {
    return delegate.getDuration(path);
  }

  @Override
  public Period getPeriod(String path) {
    return delegate.getPeriod(path);
  }

  @Override
  public TemporalAmount getTemporal(String path) {
    return delegate.getTemporal(path);
  }

  @Override
  public ConfigList getList(String path) {
    return delegate.getList(path);
  }

  @Override
  public List<Boolean> getBooleanList(String path) {
    return delegate.getBooleanList(path);
  }

  @Override
  public List<Number> getNumberList(String path) {
    return delegate.getNumberList(path);
  }

  @Override
  public List<Integer> getIntList(String path) {
    return delegate.getIntList(path);
  }

  @Override
  public List<Long> getLongList(String path) {
    return delegate.getLongList(path);
  }

  @Override
  public List<Double> getDoubleList(String path) {
    return delegate.getDoubleList(path);
  }

  @Override
  public List<String> getStringList(String path) {
    try {
      return delegate.getStringList(path);
    } catch (ConfigException.WrongType e) {
      String rawValue = getString(path);
      if (!rawValue.startsWith("[")) {
        rawValue = "[" + rawValue + "]";
      }

      // This seems a little strange. Basically, rawValue may contain null items, and we
      // want to treat those as string null, not true null. So, map such items.
      return ConfigFactory.parseString("dummyKey = " + rawValue)
          .getList("dummyKey")
          .stream()
          .map(elt -> elt.valueType() == ConfigValueType.NULL ? "null" : elt.unwrapped().toString())
          .collect(Collectors.toList());
    }
  }

  @Override
  public <T extends Enum<T>> List<T> getEnumList(Class<T> enumClass, String path) {
    return delegate.getEnumList(enumClass, path);
  }

  @Override
  public List<? extends ConfigObject> getObjectList(String path) {
    return delegate.getObjectList(path);
  }

  @Override
  public List<? extends LoaderConfig> getConfigList(String path) {
    return delegate
        .getConfigList(path)
        .stream()
        .map(DefaultLoaderConfig::new)
        .collect(Collectors.toList());
  }

  @Override
  public List<?> getAnyRefList(String path) {
    return delegate.getAnyRefList(path);
  }

  @Override
  public List<Long> getBytesList(String path) {
    return delegate.getBytesList(path);
  }

  @Override
  public List<ConfigMemorySize> getMemorySizeList(String path) {
    return delegate.getMemorySizeList(path);
  }

  @Override
  @Deprecated
  public List<Long> getMillisecondsList(String path) {
    return delegate.getMillisecondsList(path);
  }

  @Override
  @Deprecated
  public List<Long> getNanosecondsList(String path) {
    return delegate.getNanosecondsList(path);
  }

  @Override
  public List<Long> getDurationList(String path, TimeUnit unit) {
    return delegate.getDurationList(path, unit);
  }

  @Override
  public List<Duration> getDurationList(String path) {
    return delegate.getDurationList(path);
  }

  @Override
  public LoaderConfig withOnlyPath(String path) {
    return new DefaultLoaderConfig(delegate.withOnlyPath(path));
  }

  @Override
  public LoaderConfig withoutPath(String path) {
    return new DefaultLoaderConfig(delegate.withoutPath(path));
  }

  @Override
  public LoaderConfig atPath(String path) {
    return new DefaultLoaderConfig(delegate.atPath(path));
  }

  @Override
  public LoaderConfig atKey(String key) {
    return new DefaultLoaderConfig(delegate.atKey(key));
  }

  @Override
  public LoaderConfig withValue(String path, ConfigValue value) {
    return new DefaultLoaderConfig(delegate.withValue(path, value));
  }
}
