/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.services.connection;

import com.datastax.driver.dse.DseCluster;
import java.net.InetSocketAddress;
import java.util.List;

import static com.datastax.loader.services.internal.ReflectionUtils.newInstance;

/** */
@SuppressWarnings("unused")
public class ConnectionService {

  private List<String> contactPoints;

  private Policies policies;

  private ProtocolOptions protocolOptions;

  private QueryOptions queryOptions;

  private SocketOptions socketOptions;

  private AuthOptions authOptions;

  private SSLOptions sslOptions;

  private String timestampGenerator;

  private String addressTranslator;

  public DseCluster newCluster() {

    DseCluster.Builder builder = DseCluster.builder();

    contactPoints.forEach(
        s -> {
          String[] tokens = s.split(":");
          InetSocketAddress address =
              InetSocketAddress.createUnresolved(tokens[0], Integer.parseInt(tokens[1]));
          builder.addContactPointsWithPorts(address);
        });

    policies.configure(builder);

    protocolOptions.configure(builder);
    queryOptions.configure(builder);
    socketOptions.configure(builder);
    authOptions.configure(builder);
    sslOptions.configure(builder);

    builder.withTimestampGenerator(newInstance(timestampGenerator));
    builder.withAddressTranslator(newInstance(addressTranslator));

    return builder.build();
  }

  public List<String> getContactPoints() {
    return contactPoints;
  }

  public void setContactPoints(List<String> contactPoints) {
    this.contactPoints = contactPoints;
  }

  public Policies getPolicies() {
    return policies;
  }

  public void setPolicies(Policies policies) {
    this.policies = policies;
  }

  public ProtocolOptions getProtocolOptions() {
    return protocolOptions;
  }

  public void setProtocolOptions(ProtocolOptions protocolOptions) {
    this.protocolOptions = protocolOptions;
  }

  public QueryOptions getQueryOptions() {
    return queryOptions;
  }

  public void setQueryOptions(QueryOptions queryOptions) {
    this.queryOptions = queryOptions;
  }

  public SocketOptions getSocketOptions() {
    return socketOptions;
  }

  public void setSocketOptions(SocketOptions socketOptions) {
    this.socketOptions = socketOptions;
  }

  public AuthOptions getAuthOptions() {
    return authOptions;
  }

  public void setAuthOptions(AuthOptions authOptions) {
    this.authOptions = authOptions;
  }

  public SSLOptions getSslOptions() {
    return sslOptions;
  }

  public void setSslOptions(SSLOptions sslOptions) {
    this.sslOptions = sslOptions;
  }

  public String getTimestampGenerator() {
    return timestampGenerator;
  }

  public void setTimestampGenerator(String timestampGenerator) {
    this.timestampGenerator = timestampGenerator;
  }

  public String getAddressTranslator() {
    return addressTranslator;
  }

  public void setAddressTranslator(String addressTranslator) {
    this.addressTranslator = addressTranslator;
  }
}
