/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.utils;

import java.io.Closeable;
import java.util.Map;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

public class FTPUtils {
  private static final int ANY_FREE_PORT = 0;

  public static FTPTestServer createFTPServer(Map<String, String> filePathAndContent) {
    FakeFtpServer fakeFtpServer = new FakeFtpServer();
    fakeFtpServer.setServerControlPort(ANY_FREE_PORT);
    String user = "testUser";
    String password = "test";
    fakeFtpServer.addUserAccount(new UserAccount(user, password, "/"));

    FileSystem fileSystem = new UnixFakeFileSystem();

    filePathAndContent.forEach((k, v) -> fileSystem.add(new FileEntry(k, v)));
    fakeFtpServer.setFileSystem(fileSystem);

    fakeFtpServer.start();
    return new FTPTestServer(fakeFtpServer, user, password);
  }

  public static class FTPTestServer implements Closeable {
    private final FakeFtpServer fakeFtpServer;
    private final String user;
    private final String password;

    FTPTestServer(FakeFtpServer fakeFtpServer, String user, String password) {
      this.fakeFtpServer = fakeFtpServer;
      this.user = user;
      this.password = password;
    }

    @Override
    public void close() {
      fakeFtpServer.stop();
    }

    public String createConnectionString() {
      return String.format(
          "ftp://%s:%s@127.0.0.1:%d", user, password, fakeFtpServer.getServerControlPort());
    }
  }
}
