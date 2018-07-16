/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.util;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.oss.driver.internal.core.os.Native;
import com.google.common.base.Charsets;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

/**
 * This class is loosely inspired by Cassandra's {@code UUIDGen} class and by the DataStax Java
 * driver's {@code UUIDs} class.
 *
 * @see <a
 *     href="https://github.com/apache/cassandra/blob/cassandra-3.11/src/java/org/apache/cassandra/utils/UUIDGen.java">UUIDGen.java</a>
 * @see <a
 *     href="https://github.com/datastax/java-driver/blob/3.x/driver-core/src/main/java/com/datastax/driver/core/utils/UUIDs.java">UUIDs.java</a>
 * @see <a href="http://www.ietf.org/rfc/rfc4122.txt">RFC 4122</a>
 */
public enum TimeUUIDGenerator {

  /**
   * Generates version 1 UUIDs using a fixed local clock sequence and node ID.
   *
   * <p>Note that this strategy is generally only safe to use if you can guarantee that the provided
   * timestamps are unique across calls (otherwise the generated UUIDs won't be unique across
   * calls).
   */
  FIXED {

    @Override
    public UUID generate(Instant instant) {
      return new UUID(
          createMostSignificantBits(toUUIDTimestamp(instant)), FIXED_CLOCK_SEQ_AND_NODE);
    }
  },

  /**
   * Generates version 1 UUIDs using a random number in lieu of the local clock sequence and node
   * ID.
   *
   * <p>If you can guarantee that the {@code instant} argument is unique (for this JVM instance) for
   * every call, then you should prefer {@link #FIXED}, {@link #MIN} or {@link #MAX} which are
   * faster. If you can't guarantee this however, this strategy will ensure the generated UUIDs are
   * still unique (across calls) through randomization.
   */
  RANDOM {
    @Override
    public UUID generate(Instant instant) {
      return new UUID(
          createMostSignificantBits(toUUIDTimestamp(instant)),
          RANDOM_CLOCK_SEQ_AND_NODE.nextLong());
    }
  },

  /**
   * Generates the smallest possible type 1 UUID having the provided {@link Instant}.
   *
   * <p>Note that this strategy is generally only safe to use if you can guarantee that the provided
   * timestamps are unique across calls (otherwise the generated UUIDs won't be unique across
   * calls).
   */
  MIN {
    @Override
    public UUID generate(Instant instant) {
      return new UUID(createMostSignificantBits(toUUIDTimestamp(instant)), MIN_CLOCK_SEQ_AND_NODE);
    }
  },

  /**
   * Generates the biggest possible type 1 UUID having the provided {@link Instant}.
   *
   * <p>Note that this strategy is generally only safe to use if you can guarantee that the provided
   * timestamps are unique across calls (otherwise the generated UUIDs won't be unique across
   * calls).
   */
  MAX {

    @Override
    public UUID generate(Instant instant) {
      return new UUID(createMostSignificantBits(toUUIDTimestamp(instant)), MAX_CLOCK_SEQ_AND_NODE);
    }
  };

  /**
   * The timestamp is a 60-bit value. For UUID version 1, this is represented by Coordinated
   * Universal Time (UTC) as a count of 100- nanosecond intervals since 00:00:00.00, 15 October 1582
   * (the date of Gregorian reform to the Christian calendar).
   */
  public static final long EPOCH_OFFSET = 122192928000000000L;

  private static final long FIXED_CLOCK_SEQ_AND_NODE = makeClockSeqAndNode();

  /*
   * The min and max possible lsb for a UUID.
   * Note that his is not 0 and all 1's because Cassandra TimeUUIDType
   * compares the lsb parts as a signed byte array comparison. So the min
   * value is 8 times -128 and the max is 8 times +127.
   *
   * Note that we ignore the uuid variant (namely, MIN_CLOCK_SEQ_AND_NODE
   * have variant 2 as it should, but MAX_CLOCK_SEQ_AND_NODE have variant 0).
   * I don't think that has any practical consequence and is more robust in
   * case someone provides a UUID with a broken variant.
   */
  private static final long MIN_CLOCK_SEQ_AND_NODE = 0x8080808080808080L;
  private static final long MAX_CLOCK_SEQ_AND_NODE = 0x7f7f7f7f7f7f7f7fL;

  private static final Random RANDOM_CLOCK_SEQ_AND_NODE = new Random(System.currentTimeMillis());

  /**
   * Generates a version 1 time-based {@link UUID} from the provided {@link Instant}.
   *
   * @param instant the instant to use.
   * @return a version 1 time-based {@link UUID}.
   */
  public abstract UUID generate(Instant instant);

  public static long toUUIDTimestamp(Instant instant) {
    return (SECONDS.toNanos(instant.getEpochSecond()) + instant.getNano()) / 100 + EPOCH_OFFSET;
  }

  public static Instant fromUUIDTimestamp(long timestamp) {
    long n = timestamp - EPOCH_OFFSET;
    long seconds = n / 10000000L;
    long nanos = n - (seconds * 10000000L);
    return Instant.ofEpochSecond(seconds, nanos * 100);
  }

  private static long makeClockSeqAndNode() {
    long clock = new SecureRandom().nextLong();
    long lsb = 0;
    lsb |= 0x8000000000000000L; // variant (2 bits)
    lsb |= (clock & 0x0000000000003FFFL) << 48; // clock sequence (14 bits)
    lsb |= makeNode(); // 6 bytes
    return lsb;
  }

  private static long createMostSignificantBits(long nanosSince) {
    long msb = 0L;
    msb |= (0x00000000ffffffffL & nanosSince) << 32;
    msb |= (0x0000ffff00000000L & nanosSince) >>> 16;
    msb |= (0xffff000000000000L & nanosSince) >>> 48;
    msb |= 0x0000000000001000L; // sets the version to 1.
    return msb;
  }

  /**
   * We don't have access to the MAC address (in pure JAVA at least) but need to generate a node
   * part that identify this host as uniquely as possible. The spec says that one option is to take
   * as many sources that identify this node as possible and hash them together. That's what we do
   * here by gathering all the ip of this host as well as a few other sources.
   *
   * @return the node ID.
   */
  private static long makeNode() {
    try {
      MessageDigest digest = MessageDigest.getInstance("MD5");
      for (String address : getAllLocalAddresses()) {
        update(digest, address);
      }
      Properties props = System.getProperties();
      update(digest, props.getProperty("java.vendor"));
      update(digest, props.getProperty("java.vendor.url"));
      update(digest, props.getProperty("java.version"));
      update(digest, props.getProperty("os.arch"));
      update(digest, props.getProperty("os.name"));
      update(digest, props.getProperty("os.version"));
      update(digest, Integer.toString(pid()));
      byte[] hash = digest.digest();
      long node = 0;
      for (int i = 0; i < 6; i++) {
        node |= (0x00000000000000ffL & (long) hash[i]) << (i * 8);
      }
      // Since we don't use the mac address, the spec says that multicast
      // bit (least significant bit of the first byte of the node ID) must be 1.
      return node | 0x0000010000000000L;
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  private static int pid() {
    if (Native.isGetProcessIdAvailable()) {
      return Native.getProcessId();
    } else {
      try {
        String pidJmx = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        return Integer.parseInt(pidJmx);
      } catch (Exception ignored) {
        return new java.util.Random(System.currentTimeMillis()).nextInt();
      }
    }
  }

  private static void update(MessageDigest digest, String value) {
    if (value != null) {
      digest.update(value.getBytes(Charsets.UTF_8));
    }
  }

  private static Set<String> getAllLocalAddresses() {
    Set<String> allIps = new HashSet<>();
    try {
      InetAddress localhost = InetAddress.getLocalHost();
      allIps.add(localhost.toString());
      // Also return the hostname if available, it won't hurt (this does a dns lookup, it's only
      // done once at startup)
      allIps.add(localhost.getCanonicalHostName());
      InetAddress[] allMyIps = InetAddress.getAllByName(localhost.getCanonicalHostName());
      if (allMyIps != null) {
        for (InetAddress ip : allMyIps) {
          allIps.add(ip.toString());
        }
      }
    } catch (UnknownHostException e) {
      // Ignore, we'll try the network interfaces anyway
    }
    try {
      Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
      if (en != null) {
        while (en.hasMoreElements()) {
          Enumeration<InetAddress> enumIpAddr = en.nextElement().getInetAddresses();
          while (enumIpAddr.hasMoreElements()) {
            allIps.add(enumIpAddr.nextElement().toString());
          }
        }
      }
    } catch (SocketException e) {
      // Ignore, if we've really got nothing so far, we'll throw an exception
    }
    return allIps;
  }
}
