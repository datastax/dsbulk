/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.dist;

import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.loader.connectors.api.Connector;
import com.datastax.loader.engine.api.writer.ReactiveBulkWriter;
import com.datastax.loader.services.ConfigurationService;
import com.datastax.loader.services.connection.ConnectionService;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import io.reactivex.Flowable;

/** */
public class Main {

  public static void main(String[] args) {
    Main main = new Main(args);
    main.load();
  }

  private final String[] args;

  public Main(String[] args) {
    this.args = args;
  }

  public void load() {

    // load config
    ConfigurationService configurationService = new ConfigurationService();
    Config config = configurationService.loadConfig(args);

    // create services
    ConnectionService connectionService =
        ConfigBeanFactory.create(config.getConfig("connection"), ConnectionService.class);

    // create connector
    ConnectorFactory connectorFactory =
        new ConnectorFactory(config.getString("connector-class"), config.getConfig("connectors"));

    // TODO inject conversion, mapping services

    // create executor
    BulkExecutorFactory bulkExecutorFactory = new BulkExecutorFactory(config.getConfig("engine"));

    try (DseCluster cluster = connectionService.newCluster();
        DseSession session = cluster.newSession();
        Connector connector = connectorFactory.newInstance();
        ReactiveBulkWriter engine = bulkExecutorFactory.newReactiveWriter(session)) {

      connector.init();
      session.init();

      Flowable.fromPublisher(connector.read()).map(engine::writeReactive).blockingSubscribe();

      // TODO connector bad file
      // TODO executor bad file
      // TODO executor monitoring

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void unload() {
    // TODO
  }
}
