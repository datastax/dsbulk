/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.policies.lbp;

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER;
import static com.datastax.oss.driver.api.core.config.DriverExecutionProfile.DEFAULT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.dse.driver.internal.core.tracker.MultiplexingRequestTracker;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import java.net.InetSocketAddress;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;

@ExtendWith(MockitoExtension.class)
abstract class DCInferringDseLoadBalancingPolicyTestBase {

  @Mock DefaultNode node1;
  @Mock DefaultNode node2;
  @Mock DefaultNode node3;
  @Mock InternalDriverContext context;
  @Mock DriverConfig config;
  @Mock DriverExecutionProfile profile;
  @Mock Predicate<Node> filter;
  @Mock LoadBalancingPolicy.DistanceReporter distanceReporter;
  @Mock Appender<ILoggingEvent> appender;
  @Mock Request request;
  @Mock MetadataManager metadataManager;

  @Captor ArgumentCaptor<ILoggingEvent> loggingEventCaptor;

  final String logPrefix = "lbp-test-log-prefix";

  private Logger logger;

  @BeforeEach
  void setUp() {
    logger = (Logger) LoggerFactory.getLogger(DCInferringDseLoadBalancingPolicy.class);
    logger.addAppender(appender);
    given(node1.getDatacenter()).willReturn("dc1");
    given(node2.getDatacenter()).willReturn("dc1");
    given(node3.getDatacenter()).willReturn("dc1");
    given(node1.getEndPoint())
        .willReturn(new DefaultEndPoint(InetSocketAddress.createUnresolved("1.1.1.1", 9042)));
    given(node2.getEndPoint())
        .willReturn(new DefaultEndPoint(InetSocketAddress.createUnresolved("1.1.1.2", 9042)));
    given(node3.getEndPoint())
        .willReturn(new DefaultEndPoint(InetSocketAddress.createUnresolved("1.1.1.3", 9042)));
    given(filter.test(any(Node.class))).willReturn(true);
    given(context.getNodeFilter(DEFAULT_NAME)).willReturn(filter);
    given(context.getConfig()).willReturn(config);
    given(config.getProfile(DEFAULT_NAME)).willReturn(profile);
    given(profile.getString(LOAD_BALANCING_LOCAL_DATACENTER, null)).willReturn("dc1");
    given(context.getMetadataManager()).willReturn(metadataManager);
    given(context.getRequestTracker()).willReturn(new MultiplexingRequestTracker());
  }

  @AfterEach
  void tearDown() {
    logger.detachAppender(appender);
  }
}
