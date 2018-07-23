package py.infocenter.test.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperEnvironmentTool {
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperEnvironmentTool.class);
    private String zookeeperPackage = "zookeeper-3.4.6";
    private String currentWorkPath;
    private Map<Integer, Process> zooKeeperProcesses;

    public ZookeeperEnvironmentTool() {
        zooKeeperProcesses = new HashMap<Integer, Process>();
    }

    public void build() throws Exception {
        // create three zookeeper for test
        currentWorkPath = System.getProperty("user.dir");
        Process process;
        String cmd;

        String zooKeeperPath = currentWorkPath + "/../resources/packages/" + zookeeperPackage + ".tar.gz";
        cmd = "tar -xvf " + zooKeeperPath + " -C /tmp/";
        logger.debug("command: {}", cmd);
        process = Runtime.getRuntime().exec(cmd);
        StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "Error");
        StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream(), "Output");
        errorGobbler.start();
        outputGobbler.start();

        process.waitFor();
        process.destroy();

        cmd = "/tmp/" + zookeeperPackage + "/bin/zkServer.sh start ";
        // create workspace for zookeeper
        for (int i = 1; i <= 3; i++) {
            String configPath = currentWorkPath + "/src/test/resources/" + "zoo" + i + ".cfg";
            File file = new File(configPath);
            if (!file.exists()) {
                logger.error("file " + configPath + " not exist");
                continue;
            }

            createWorkspaceForZookeeper(i);
            // start zookeeper service
            String tmp_command = cmd + configPath;
            logger.debug("command: {}", tmp_command);
            process = Runtime.getRuntime().exec(tmp_command);
            errorGobbler = new StreamGobbler(process.getErrorStream(), "Error");
            outputGobbler = new StreamGobbler(process.getInputStream(), "Output");
            errorGobbler.start();
            outputGobbler.start();

            zooKeeperProcesses.put(i, process);
        }

    }

    public void shutdownAllZookeeperServer() {
        for (int i = 0; i <= 3; i++) {
            shutdownZookeeperServer(i);
        }
    }

    public void shutdownZookeeperServer(int i) {
        String cmd;
        Process process;
        cmd = "/tmp/" + zookeeperPackage + "/bin/zkServer.sh stop ";
        String configPath = currentWorkPath + "/src/test/resources/" + "zoo" + i + ".cfg";
        logger.debug("command: {} {}", cmd, configPath);
        try {
            process = Runtime.getRuntime().exec(cmd + configPath);
            process.waitFor();
            process.destroy();
        } catch (Exception e) {
            logger.info("stop services failure");
        }
    }

    public void destory() throws Exception {
        String cmd;
        Process process;
        cmd = "/tmp/" + zookeeperPackage + "/bin/zkServer.sh stop ";
        for (int i = 1; i <= 3; i++) {
            String configPath = currentWorkPath + "/src/test/resources/" + "zoo" + i + ".cfg";
            try {
                logger.debug("command: {} {}", cmd, configPath);
                process = Runtime.getRuntime().exec(cmd + configPath);
                process.waitFor();
                process.destroy();
            } catch (Exception e) {
                logger.info("stop services failure");
            }
        }

        for (Entry<Integer, Process> tmp : zooKeeperProcesses.entrySet()) {
            try {
                tmp.getValue().destroy();
            } catch (Exception e) {
                logger.info("kill process failure");
            }
        }

        cmd = "rm " + currentWorkPath + "/zookeeper.log";
        logger.debug("command: {}",cmd);
        process = Runtime.getRuntime().exec(cmd);
        process.waitFor();
        process.destroy();

        cmd = "rm " + currentWorkPath + "/zookeeper.out";
        logger.debug("command: {}",cmd);
        process = Runtime.getRuntime().exec(cmd);
        process.waitFor();
        process.destroy();

        cmd = "rm -r " + "/tmp/test";
        logger.debug("command: {}",cmd);
        process = Runtime.getRuntime().exec(cmd);
        process.waitFor();
        process.destroy();

        cmd = "rm " + "/tmp/" + zookeeperPackage + ".tar.gz";
        logger.debug("command: {}",cmd);
        process = Runtime.getRuntime().exec(cmd);
        process.waitFor();
        process.destroy();

        cmd = "rm -r " + "/tmp/" + zookeeperPackage;
        logger.debug("command: {}",cmd);
        process = Runtime.getRuntime().exec(cmd);
        process.waitFor();
        process.destroy();
    }

    private void createWorkspaceForZookeeper(int index) throws Exception {
        File file;
        String cmd;
        String dir;
        dir = "/tmp/test/zookeeper" + index + "/data";
        file = new File(dir);
        if (!file.exists()) {
            file.mkdirs();
        }

        cmd = "echo " + index + " > " + dir + "/myid";
        String[] commands = { "bash", "-c", cmd };
        Runtime.getRuntime().exec(commands);

        dir = "/tmp/test/zookeeper" + index + "/logs";
        file = new File(dir);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    private class StreamGobbler extends Thread {
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
