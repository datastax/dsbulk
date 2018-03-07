/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.tck;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.google.common.reflect.TypeToken;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

class MockRow implements Row {

  private int index;

  MockRow(int index) {
    this.index = index;
  }

  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return null;
  }

  @Override
  public ByteBuffer getBytesUnsafe(int i) {
    return null;
  }

  @Override
  public Token getToken(int i) {
    return null;
  }

  @Override
  public Token getToken(String name) {
    return null;
  }

  @Override
  public Token getPartitionKeyToken() {
    return null;
  }

  @Override
  public boolean isNull(int i) {
    return false;
  }

  @Override
  public boolean getBool(int i) {
    return false;
  }

  @Override
  public byte getByte(int i) {
    return 0;
  }

  @Override
  public short getShort(int i) {
    return 0;
  }

  @Override
  public int getInt(int i) {
    return 0;
  }

  @Override
  public long getLong(int i) {
    return 0;
  }

  @Override
  public Date getTimestamp(int i) {
    return null;
  }

  @Override
  public LocalDate getDate(int i) {
    return null;
  }

  @Override
  public long getTime(int i) {
    return 0;
  }

  @Override
  public float getFloat(int i) {
    return 0;
  }

  @Override
  public double getDouble(int i) {
    return 0;
  }

  @Override
  public ByteBuffer getBytes(int i) {
    return null;
  }

  @Override
  public String getString(int i) {
    return null;
  }

  @Override
  public BigInteger getVarint(int i) {
    return null;
  }

  @Override
  public BigDecimal getDecimal(int i) {
    return null;
  }

  @Override
  public UUID getUUID(int i) {
    return null;
  }

  @Override
  public InetAddress getInet(int i) {
    return null;
  }

  @Override
  public <T> List<T> getList(int i, Class<T> elementsClass) {
    return null;
  }

  @Override
  public <T> List<T> getList(int i, TypeToken<T> elementsType) {
    return null;
  }

  @Override
  public <T> Set<T> getSet(int i, Class<T> elementsClass) {
    return null;
  }

  @Override
  public <T> Set<T> getSet(int i, TypeToken<T> elementsType) {
    return null;
  }

  @Override
  public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass) {
    return null;
  }

  @Override
  public <K, V> Map<K, V> getMap(int i, TypeToken<K> keysType, TypeToken<V> valuesType) {
    return null;
  }

  @Override
  public UDTValue getUDTValue(int i) {
    return null;
  }

  @Override
  public TupleValue getTupleValue(int i) {
    return null;
  }

  @Override
  public Object getObject(int i) {
    return null;
  }

  @Override
  public <T> T get(int i, Class<T> targetClass) {
    return null;
  }

  @Override
  public <T> T get(int i, TypeToken<T> targetType) {
    return null;
  }

  @Override
  public <T> T get(int i, TypeCodec<T> codec) {
    return null;
  }

  @Override
  public boolean isNull(String name) {
    return false;
  }

  @Override
  public boolean getBool(String name) {
    return false;
  }

  @Override
  public byte getByte(String name) {
    return 0;
  }

  @Override
  public short getShort(String name) {
    return 0;
  }

  @Override
  public int getInt(String name) {
    return 0;
  }

  @Override
  public long getLong(String name) {
    return 0;
  }

  @Override
  public Date getTimestamp(String name) {
    return null;
  }

  @Override
  public LocalDate getDate(String name) {
    return null;
  }

  @Override
  public long getTime(String name) {
    return 0;
  }

  @Override
  public float getFloat(String name) {
    return 0;
  }

  @Override
  public double getDouble(String name) {
    return 0;
  }

  @Override
  public ByteBuffer getBytesUnsafe(String name) {
    return null;
  }

  @Override
  public ByteBuffer getBytes(String name) {
    return null;
  }

  @Override
  public String getString(String name) {
    return null;
  }

  @Override
  public BigInteger getVarint(String name) {
    return null;
  }

  @Override
  public BigDecimal getDecimal(String name) {
    return null;
  }

  @Override
  public UUID getUUID(String name) {
    return null;
  }

  @Override
  public InetAddress getInet(String name) {
    return null;
  }

  @Override
  public <T> List<T> getList(String name, Class<T> elementsClass) {
    return null;
  }

  @Override
  public <T> List<T> getList(String name, TypeToken<T> elementsType) {
    return null;
  }

  @Override
  public <T> Set<T> getSet(String name, Class<T> elementsClass) {
    return null;
  }

  @Override
  public <T> Set<T> getSet(String name, TypeToken<T> elementsType) {
    return null;
  }

  @Override
  public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
    return null;
  }

  @Override
  public <K, V> Map<K, V> getMap(String name, TypeToken<K> keysType, TypeToken<V> valuesType) {
    return null;
  }

  @Override
  public UDTValue getUDTValue(String name) {
    return null;
  }

  @Override
  public TupleValue getTupleValue(String name) {
    return null;
  }

  @Override
  public Object getObject(String name) {
    return null;
  }

  @Override
  public <T> T get(String name, Class<T> targetClass) {
    return null;
  }

  @Override
  public <T> T get(String name, TypeToken<T> targetType) {
    return null;
  }

  @Override
  public <T> T get(String name, TypeCodec<T> codec) {
    return null;
  }

  // equals and hashCode required for TCK tests that check that two subscribers
  // receive the exact same set of items.

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MockRow mockRow = (MockRow) o;
    return index == mockRow.index;
  }

  @Override
  public int hashCode() {
    return index;
  }
}
