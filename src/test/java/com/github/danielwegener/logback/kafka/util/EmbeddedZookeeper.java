package com.github.danielwegener.logback.kafka.util;

import static org.junit.Assert.assertEquals;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;

public class EmbeddedZookeeper {
    private int port = -1;
    private int tickTime = 100;

    private ServerCnxnFactory factory;
    private File snapshotDir;
    private File logDir;

    public EmbeddedZookeeper() {
        this(-1);
    }

    public EmbeddedZookeeper(int port) {
        this(port, 500);
    }

    public EmbeddedZookeeper(int port, int tickTime) {
        this.port = resolvePort(port);
        this.tickTime = tickTime;
    }

    private int resolvePort(int port) {
        if (port == -1) {
            return TestUtils.getAvailablePort();
        }
        return port;
    }

    public void startup() throws IOException{
        if (this.port == -1) {
            this.port = TestUtils.getAvailablePort();
        }
        this.factory = NIOServerCnxnFactory.createFactory(new InetSocketAddress("localhost", port), 1024);
        this.snapshotDir = TestUtils.constructTempDir("embeeded-zk/snapshot");
        this.logDir = TestUtils.constructTempDir("embeeded-zk/log");
        final ZooKeeperServer zooKeeperServer = new ZooKeeperServer(snapshotDir, logDir, tickTime);
        try {
            factory.startup(zooKeeperServer);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        assertEquals("standalone", zooKeeperServer.getState());
        assertEquals(this.port, zooKeeperServer.getClientPort());

    }


    public void shutdown() {
        factory.shutdown();
        try {
            factory.join();
        } catch (InterruptedException e) {
            throw new IllegalStateException("should not happen: "+e.getMessage(), e);
        }
        try {
            TestUtils.deleteFile(snapshotDir);
        } catch (FileNotFoundException e) {
            // ignore
        }
        try {
            TestUtils.deleteFile(logDir);
        } catch (FileNotFoundException e) {
            // ignore
        }
    }

    public String getConnection() {
        return "localhost:" + port;
    }


    @Override
    public String toString() {
        return "EmbeddedZookeeper{" + "connection=" + getConnection() + '}';
    }
}
