package com.datastax.dsbulk.commons.tests.utils;

import java.io.Closeable;
import java.util.Map;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

public class FtpUtils {

  public static FtpTestServer createFtpServer(Map<String, String> filePathAndContent) {
    FakeFtpServer fakeFtpServer = new FakeFtpServer();
    fakeFtpServer.setServerControlPort(43121);
    String user = "testUser";
    String password = "test";
    fakeFtpServer.addUserAccount(new UserAccount(user, password, "/"));

    FileSystem fileSystem = new UnixFakeFileSystem();

    filePathAndContent.forEach((k, v) -> fileSystem.add(new FileEntry(k, v)));
    fakeFtpServer.setFileSystem(fileSystem);

    fakeFtpServer.start();
    return new FtpTestServer(fakeFtpServer, user, password);
  }

  public static class FtpTestServer implements Closeable {
    private final FakeFtpServer fakeFtpServer;
    private final String user;
    private final String password;

    FtpTestServer(FakeFtpServer fakeFtpServer, String user, String password) {
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
