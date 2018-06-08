/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.log;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * A sink for log messages.
 *
 * <p>This interface is intended to act as a tiny wrapper around calls to the underlying logging
 * subsystem.
 *
 * <p>A typical implementation is:
 *
 * <pre>
 * Logger logger = LoggerFactory.getLogger(...);
 * LogSink sink = new LogSink() {
 *
 *       public boolean isEnabled() {
 *         return logger.isInfoEnabled();
 *       }
 *
 *
 *       public void accept(String message, Object... args) {
 *         logger.info(message, args);
 *       }
 *     };
 * </pre>
 */
@FunctionalInterface
public interface LogSink {

  /**
   * Creates a new sink with the given enablement supplier and given message consumer.
   *
   * <p>A typical invokation of this method is:
   *
   * <pre>
   * Logger logger = LoggerFactory.getLogger(...);
   * LogSink sink = LogSink.buildFrom(logger::isInfoEnabled, logger::info);
   * </pre>
   *
   * @param enablementSupplier an enablement supplier.
   * @param messageConsumer a message consumer.
   * @return a newly-allocated sink.
   */
  static LogSink buildFrom(
      Supplier<Boolean> enablementSupplier, BiConsumer<String, Object[]> messageConsumer) {
    return new LogSink() {

      @Override
      public boolean isEnabled() {
        return enablementSupplier.get();
      }

      @Override
      public void accept(String message, Object... args) {
        messageConsumer.accept(message, args);
      }
    };
  }

  /**
   * Whether the sink will effectively process the message (i.e., log it) or not.
   *
   * @return true if the sink will process the message, false if it will reject it.
   */
  default boolean isEnabled() {
    return true;
  }

  /**
   * Process the given message by (possibly) logging it.
   *
   * @param message The message to process.
   * @param args The (optional) message arguments.
   */
  void accept(String message, Object... args);
}
