package py.infocenter.zookeeper;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import py.app.context.AppContext;
import py.app.context.AppContextImpl;
import py.common.struct.EndPoint;
import py.instance.InstanceStatus;
import py.instance.PortType;
import py.test.TestBase;
import py.zookeeper.ZkClient;
import py.zookeeper.ZkClientFactory;
import py.zookeeper.ZkClientImpl;
import py.zookeeper.ZkElectionLeader;
import py.zookeeper.ZkException;
import py.zookeeper.ZkListener;

public class ZkClientIntegTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(ZkClientIntegTest.class);
    private int sessionTimeout = 4000;
    private static String zookeeperPackage = "zookeeper-3.4.6";
    private static String currentWorkPath;
    private List<Integer> ports;
    private List<Process> processes;

    public ZkClientIntegTest() throws Exception {
        super.init();
    }

    @Before
    public void init() throws Exception {
        // create three zookeeper for test
        currentWorkPath = System.getProperty("user.dir");
        Process process;
        String cmd;

        String zooKeeperPath = currentWorkPath + "/../resources/packages/" + zookeeperPackage + ".tar.gz";
        cmd = "tar -xvf " + zooKeeperPath + " -C /tmp/";
        process = Runtime.getRuntime().exec(cmd);
        StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "Error");
        StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream(), "Output");
        errorGobbler.start();
        outputGobbler.start();

        process.waitFor();
        process.destroy();

        cmd = "/tmp/" + zookeeperPackage + "/bin/zkServer.sh start ";
        ports = new ArrayList<Integer>();
        processes = new ArrayList<Process>();

        // create workspace for zookeeper
        for (int i = 0; i < 3; i++) {
            String configPath = currentWorkPath + "/src/test/resources/" + "zoo" + (i + 1) + ".cfg";
            File file = new File(configPath);
            if (!file.exists()) {
                logger.error("file " + configPath + " not exist");
                fail();
            }

            // read the client port
            Properties prop = new Properties();
            InputStream inputStream = new FileInputStream(configPath);
            prop.load(inputStream);
            ports.add(Integer.valueOf(prop.getProperty("clientPort")));
            inputStream.close();

            // stop zookeeper service
            killZkService(i, true);
            
            // start zookeeper service
            startZkService(i, true);
        }
    }

    @Test
    public void testZkClient() throws Exception {
        String root = "/infocenter/test1/";
        TestZkClient testZkClient = getZkClient(getFactory(0), null, root);
        
        ZkClient zkClient = testZkClient.getClient();
        zkClient.createPath(root);
        List<String> files = zkClient.getFiles(root);
        assertTrue("files: " + files, files.isEmpty());

        int dataLength = 10;
        byte[] data = new byte[dataLength];
        for (int i = 0; i < dataLength; i++) {
            data[i] = (byte) i;
        }

        int nodeCount = 5;
        for (int i = 0; i < nodeCount; i++) {
            String node = zkClient.createFile(root + i, data, i % 2 == 0);
            logger.info("current node: " + node);
        }

        files = zkClient.getFiles(root);
        assertTrue("files: " + files + ", expected: " + nodeCount, files.size() == nodeCount);

        for (String name : files) {
            data = zkClient.readData(root + name);
            assertTrue(data.length == dataLength);
            for (int i = 0; i < dataLength; i++) {
                assertTrue(data[i] == (byte) i);
            }
        }
    }

    @Test
    public void testMonitorDeleted() throws Exception {
        String root = "/infocenter/test2/";
        final List<String> deleteFiles = new ArrayList<String>();
        final CountDownLatch latch = new CountDownLatch(2);
        ZkListener listener = new ZkListener() {

            @Override
            public void pathDeleted(String path) {
                logger.info("delete path: {}", path);
                deleteFiles.add(path);
                latch.countDown();
            }

            @Override
            public void disconnected() {
            }

            @Override
            public void connected(long sessionId) {
            }

            @Override
            public void expired() {
            }
        };

        TestZkClient testZkClient = getZkClient(getFactory(0), listener, root);
        ZkClient zkClient = testZkClient.getClient();
        String ticket1 = zkClient.createFile(root + "index_", null, true);
        String ticket2 = zkClient.createFile(root + "index_", null, true);
        String ticket3 = zkClient.createFile(root + "test1_", null, false);
        String ticket4 = zkClient.createFile(root + "test2_", null, false);

        zkClient.monitor(ticket2);
        zkClient.monitor(ticket3);

        zkClient.deleteFile(ticket1);
        zkClient.deleteFile(ticket2);
        zkClient.deleteFile(ticket3);
        zkClient.deleteFile(ticket4);
        latch.await();

        assertTrue("delete tickets: " + deleteFiles, !deleteFiles.contains(ticket1));
        assertTrue("delete tickets: " + deleteFiles, deleteFiles.contains(ticket2));
        assertTrue("delete tickets: " + deleteFiles, deleteFiles.contains(ticket3));
        assertTrue("delete tickets: " + deleteFiles, !deleteFiles.contains(ticket4));
    }
    
    public class Container {
        public ZkClient zkClient;
        public List<ZkListener> zkListeners;
    }

    @Test
    public void testReconnection() throws Exception {
        final Container container = new Container();
        final CountDownLatch connection = new CountDownLatch(1);
        final CountDownLatch disconnection = new CountDownLatch(1);
        final CountDownLatch reconnection = new CountDownLatch(1);
        final int index = 0;
        ZkListener listener = new ZkListener() {
            @Override
            public void pathDeleted(String path) {
            }

            @Override
            public void disconnected() {
                logger.info("disconnected");
                synchronized (this) {
                    try {
                        container.zkClient.close();
                        container.zkClient = new ZkClientImpl("localhost:" + ports.get(index), sessionTimeout, container.zkListeners);
                    } catch (ZkException e) {
                        logger.error("close failure", e);
                    }
                }
                disconnection.countDown();
            }

            @Override
            public void expired() {
                logger.info("expired");
            }

            @Override
            public void connected(long sessionId) {
                logger.info("connected");
                if (connection.getCount() == 1) {
                    connection.countDown();
                } else {
                    reconnection.countDown();
                }
            }
        };

        List<ZkListener> listeners = new ArrayList<ZkListener>();
        listeners.add(listener);
        container.zkListeners = listeners;
        container.zkClient = new ZkClientImpl("localhost:" + ports.get(index), sessionTimeout, listeners);
        assertTrue(connection.await(10, TimeUnit.SECONDS));
        killZkService(index, true);
        assertTrue(disconnection.await(10, TimeUnit.SECONDS));
        Thread.sleep(3000);
        startZkService(index, true);
        assertTrue(reconnection.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testReconnection1() throws Exception {
        String root = "/infocenter/test3/";
        int index = 0;

        TestZkClient testZkClient = getZkClient(getFactory(index), null, root);
        String nodeName = "node1";
        ZkClient zkClient = testZkClient.getClient();
        zkClient.createFile(root + nodeName, null, false);
        killZkService(index, false);
        testZkClient.waitDisconnected(10000);
        startZkService(index, false);
        testZkClient.waitConnnected(10000);

        zkClient = testZkClient.getClient();
        List<String> files = zkClient.getFiles(root);
        logger.info("files: " + files);
        assertTrue(files.size() == 1);
        assertTrue(nodeName.equals(files.get(0)));
    }

    @Test
    public void testReconnection1WhenCleanEnv() throws Exception {
        String root = "/infocenter/test4/";
        int index = 0;

        TestZkClient testZkClient = getZkClient(getFactory(index), null, root);
        String nodeName = "node1";
        ZkClient zkClient = testZkClient.getClient();
        zkClient.createFile(root + nodeName, null, false);
        killZkService(index, true);
        testZkClient.waitDisconnected(10000);
        Thread.sleep(5000);
        startZkService(index, true);
        testZkClient.waitConnnected(10000);

        zkClient = testZkClient.getClient();
        List<String> files = zkClient.getFiles(root);
        logger.info("files: " + files);
        assertTrue(files.size() == 1);
        assertTrue(nodeName.equals(files.get(0)));
    }

    @Test
    public void testReconnection2() throws Exception {
        String root = "/infocenter/test5/";
        int index = 0;

        TestZkClient testZkClient = getZkClient(getFactory(index), null, root);

        ZkClient zkClient = testZkClient.getClient();
        String nodeName = "node1";
        zkClient.createFile(root + nodeName, null, false);
        for (int i = 0; i < processes.size(); i++) {
            killZkService(i, false);
        }

        Thread.sleep(5000);
        testZkClient.waitDisconnected(10000);
        for (int i = 0; i < processes.size(); i++) {
            startZkService(i, false);
        }

        testZkClient.waitConnnected(10000);
        zkClient = testZkClient.getClient();
        List<String> files = zkClient.getFiles(root);
        logger.info("files: " + files);
        assertTrue(files.size() == 1);
        assertTrue(nodeName.equals(files.get(0)));
    }

    @Test
    public void testReconnection2WhenCleanEnv() throws Exception {
        String root = "/infocenter/test6/";
        int index = 0;

        TestZkClient testZkClient = getZkClient(getFactory(index), null, root);

        ZkClient zkClient = testZkClient.getClient();
        String nodeName = "node1";
        zkClient.createFile(root + nodeName, null, false);

        for (int i = 0; i < processes.size(); i++) {
            killZkService(i, true);
        }

        Thread.sleep(5000);
        testZkClient.waitDisconnected(10000);
        for (int i = 0; i < processes.size(); i++) {
            startZkService(i, true);
        }

        testZkClient.waitConnnected(10000);
        List<String> files = zkClient.getFiles(root);
        logger.info("files: " + files);
        assertTrue(files.size() == 1);
        assertTrue(nodeName.equals(files.get(0)));
    }

    @Test
    public void testElectionLeader() throws Exception {
        String root = "/infocenter/test5/";
        List<ZkElectionLeader> leaders = new ArrayList<ZkElectionLeader>();
        for (Integer port : ports) {
            ZkElectionLeader leader = new ZkElectionLeader(new ZkClientFactory("localhost:" + port, sessionTimeout),
                            root, appContext());
            leader.startElection();
            Thread.sleep(2000);
            leaders.add(leader);
        }

        Thread.sleep(5000);
        assertTrue(leaders.get(0).getAppContext().getStatus() == InstanceStatus.OK);
        assertTrue(leaders.get(1).getAppContext().getStatus() == InstanceStatus.SUSPEND);
        assertTrue(leaders.get(2).getAppContext().getStatus() == InstanceStatus.SUSPEND);

        logger.info("@@ kill index:0");
        killZkService(0, false);
        Thread.sleep(5000);
        logger.info("1 status 1:{}, 2:{}, 3:{}", leaders.get(0).getAppContext().getStatus(), leaders.get(1)
                        .getAppContext().getStatus(), leaders.get(2).getAppContext().getStatus());
        assertTrue(leaders.get(0).getAppContext().getStatus() == InstanceStatus.SUSPEND);
        assertTrue(leaders.get(1).getAppContext().getStatus() == InstanceStatus.OK);
        assertTrue(leaders.get(2).getAppContext().getStatus() == InstanceStatus.SUSPEND);

        logger.info("@@ start index:0");
        startZkService(0, false);
        Thread.sleep(10000);
        logger.info("2 status 1:{}, 2:{}, 3:{}", leaders.get(0).getAppContext().getStatus(), leaders.get(1)
                        .getAppContext().getStatus(), leaders.get(2).getAppContext().getStatus());
        assertTrue(leaders.get(0).getAppContext().getStatus() == InstanceStatus.SUSPEND);
        assertTrue(leaders.get(1).getAppContext().getStatus() == InstanceStatus.OK);
        assertTrue(leaders.get(2).getAppContext().getStatus() == InstanceStatus.SUSPEND);

        logger.info("@@ kill index:1");
        killZkService(1, false);
        Thread.sleep(10000);
        logger.info("3 status 1:{}, 2:{}, 3:{}", leaders.get(0).getAppContext().getStatus(), leaders.get(1)
                        .getAppContext().getStatus(), leaders.get(2).getAppContext().getStatus());
        assertTrue(leaders.get(0).getAppContext().getStatus() == InstanceStatus.SUSPEND);
        assertTrue(leaders.get(1).getAppContext().getStatus() == InstanceStatus.SUSPEND);
        assertTrue(leaders.get(2).getAppContext().getStatus() == InstanceStatus.OK);

        logger.info("@@ start index:1");
        startZkService(1, false);
        Thread.sleep(10000);
        logger.info("4 status 1:{}, 2:{}, 3:{}", leaders.get(0).getAppContext().getStatus(), leaders.get(1)
                        .getAppContext().getStatus(), leaders.get(2).getAppContext().getStatus());
        assertTrue(leaders.get(0).getAppContext().getStatus() == InstanceStatus.SUSPEND);
        assertTrue(leaders.get(1).getAppContext().getStatus() == InstanceStatus.SUSPEND);
        assertTrue(leaders.get(2).getAppContext().getStatus() == InstanceStatus.OK);

        logger.info("@@ kill index:2");
        killZkService(2, false);
        Thread.sleep(10000);
        logger.info("5 status 1:{}, 2:{}, 3:{}", leaders.get(0).getAppContext().getStatus(), leaders.get(1)
                        .getAppContext().getStatus(), leaders.get(2).getAppContext().getStatus());
        assertTrue(leaders.get(0).getAppContext().getStatus() == InstanceStatus.OK);
        assertTrue(leaders.get(1).getAppContext().getStatus() == InstanceStatus.SUSPEND);
        assertTrue(leaders.get(2).getAppContext().getStatus() == InstanceStatus.SUSPEND);
    }

    private class TestZkClient implements ZkListener {
        private CountDownLatch connectLatch;
        private CountDownLatch disconnectLatch;
        private ZkClient zkClient;
        private ZkClientFactory factory;
        private List<ZkListener> listeners;

        public TestZkClient(ZkClientFactory factory, ZkListener listener) throws Exception {
            this.connectLatch = new CountDownLatch(1);
            this.disconnectLatch = new CountDownLatch(1);
            this.factory = factory;
            this.listeners = new ArrayList<ZkListener>();
            if (listener != null) {
                this.listeners.add(listener);
            }
            this.listeners.add(this);
            this.zkClient = factory.generate(this.listeners);
        }

        public ZkClient getClient() {
            return zkClient;
        }

        public void waitConnnected(long timeoutMs) throws Exception {
            assertTrue(connectLatch.await(timeoutMs, TimeUnit.MILLISECONDS));
            connectLatch = new CountDownLatch(1);
        }

        public void waitDisconnected(long timeoutMs) throws Exception {
            assertTrue(disconnectLatch.await(timeoutMs, TimeUnit.MILLISECONDS));
            disconnectLatch = new CountDownLatch(1);
        }

        @Override
        public void pathDeleted(String path) {
        }

        @Override
        public void disconnected() {
            if (disconnectLatch != null) {
                logger.info("++ disconnected");
                disconnectLatch.countDown();
            }
        }

        @Override
        public void expired() {
            logger.info("++ expired");
            try {
                this.zkClient = factory.generate(listeners);
            } catch (ZkException e) {
                logger.info("can not get zkclient",factory.getServerAddress());
            }
        }

        @Override
        public void connected(long sessionId) {
            if (connectLatch != null) {
                logger.info("++ connected");
                connectLatch.countDown();
            }
        }
    }

    private ZkClientFactory getFactory(int index) {
        return new ZkClientFactory("localhost:" + ports.get(index), sessionTimeout);
    }

    private TestZkClient getZkClient(ZkClientFactory factory, ZkListener listener, String root) throws Exception {
        TestZkClient client = new TestZkClient(factory, listener);
        client.waitConnnected(10000);
        ZkClient zkClient = client.getClient();
        zkClient.createPath(root);
        return client;
    }

    private void killZkService(int index, boolean deleteEnv) throws Exception {
        String cmd = "/tmp/" + zookeeperPackage + "/bin/zkServer.sh stop " + currentWorkPath
                        + "/src/test/resources/zoo" + (index + 1) + ".cfg";
        Process process = Runtime.getRuntime().exec(cmd);
        process.waitFor();
        process.destroy();
        
        if (deleteEnv) {
            dealWithEnv(index,false);
        }
    }

    private void startZkService(int index, boolean createEnv) throws Exception {
        if (createEnv) {
            dealWithEnv(index,true);
        }

        String cmd = "/tmp/" + zookeeperPackage + "/bin/zkServer.sh start " + currentWorkPath
                        + "/src/test/resources/zoo" + (index + 1) + ".cfg";
        Process process = Runtime.getRuntime().exec(cmd);
        process.waitFor();
        StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "Error");
        StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream(), "Output");
        errorGobbler.start();
        outputGobbler.start();
        if (index < processes.size()) {
            processes.set(index, process);
        } else {
            processes.add(process);
            assertTrue(processes.size() == index + 1);
        }
        logger.info("start process: {} and listen in port: {}", process, ports.get(index));
    }

    @After
    public void close() throws Exception {
        String cmd;
        Process process;
        cmd = "/tmp/" + zookeeperPackage + "/bin/zkServer.sh stop ";
        for (int i = 1; i <= 3; i++) {
            String configPath = currentWorkPath + "/src/test/resources/" + "zoo" + i + ".cfg";
            try {
                process = Runtime.getRuntime().exec(cmd + configPath);
                process.waitFor();
                process.destroy();
            } catch (Exception e) {
                logger.info("stop services failure");
            }
        }

        for (Process tmp : processes) {
            try {
                tmp.destroy();
            } catch (Exception e) {
                logger.info("kill process failure");
            }
        }

        cmd = "rm " + currentWorkPath + "/zookeeper.log";
        process = Runtime.getRuntime().exec(cmd);
        process.waitFor();
        process.destroy();

        cmd = "rm " + currentWorkPath + "/zookeeper.out";
        process = Runtime.getRuntime().exec(cmd);
        process.waitFor();
        process.destroy();

        cmd = "rm -r " + "/tmp/test";
        process = Runtime.getRuntime().exec(cmd);
        process.waitFor();
        process.destroy();

        cmd = "rm " + "/tmp/" + zookeeperPackage + ".tar.gz";
        process = Runtime.getRuntime().exec(cmd);
        process.waitFor();
        process.destroy();

        cmd = "rm -r " + "/tmp/" + zookeeperPackage;
        process = Runtime.getRuntime().exec(cmd);
        process.waitFor();
        process.destroy();
    }

    private void dealWithEnv(int index, boolean createOrDelete) throws Exception {
        File file;
        String root = "/tmp/test/zookeeper" + (index + 1) + "/";
        if (createOrDelete) {
            String dir = root + "data";
            file = new File(dir);
            if (!file.exists()) {
                file.mkdirs();
            }

            String cmd = "echo " + (index + 1) + " > " + dir + "/myid";
            String[] commands = { "bash", "-c", cmd };
            Runtime.getRuntime().exec(commands);

            dir = root + "logs";
            file = new File(dir);
            if (!file.exists()) {
                file.mkdirs();
            }
        } else {
            file = new File(root + "data"); 
            if (file.exists()) {
                FileUtils.deleteDirectory(file);
            }
        }
    }

    public AppContext appContext() {
        AppContextImpl appContext = new AppContextImpl("InfoCenter");
        appContext.putEndPoint(PortType.CONTROL, new EndPoint(null, 10000));
        appContext.setLocation("c=r1;d=dc");
        appContext.setInstanceIdStore(null);
        appContext.setStatus(InstanceStatus.SUSPEND);
        return appContext;
    }

    class StreamGobbler extends Thread {
        InputStream is;
        String type;

        StreamGobbler(InputStream is, String type) {
            this.is = is;
            this.type = type;
        }

        public void run() {
            try {
                InputStreamReader isr = new InputStreamReader(is);
                BufferedReader br = new BufferedReader(isr);
                String line = null;
                while ((line = br.readLine()) != null) {
                    if (type.equals("Error")) {
                        logger.info(line);
                    } else {
                        // logger.info(line);
                    }
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }
}
