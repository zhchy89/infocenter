package py.infocenter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import py.app.thrift.ThriftAppEngine;
import py.infocenter.service.InformationCenterImpl;
import py.periodic.PeriodicWorkExecutor;
import py.periodic.impl.PeriodicWorkExecutorImpl;
import py.zookeeper.ZkElectionLeader;

public class InformationCenterAppEngine extends ThriftAppEngine {
    private final static Logger logger = LoggerFactory.getLogger(InformationCenterAppEngine.class);

    private PeriodicWorkExecutorImpl driverStoreSweeperExecutor;
    private PeriodicWorkExecutorImpl volumeSweeperExecutor;
    private PeriodicWorkExecutorImpl instanceMetadataStoreSweeperExecutor;
    private PeriodicWorkExecutorImpl timeoutSweeperExecutor;
    private PeriodicWorkExecutorImpl alarmSweeperExecutor;
    private PeriodicWorkExecutorImpl serverNodeAlertCheckerExecutor;

    private InformationCenterImpl informationCenterImpl;
    private ZkElectionLeader zkElectionLeader;
    private InformationCenterAppConfig informationCenterAppConfig;

    public InformationCenterAppEngine(InformationCenterImpl informationCenterImpl) {
        super(informationCenterImpl);
        informationCenterImpl.setInformationCenterAppEngine(this);
        this.informationCenterImpl = informationCenterImpl;
    }

    public void start() throws Exception {
        /**
         * if the switch of the election leader is opened, we should check if we can cast ticket.
         */
        if (informationCenterAppConfig.getZookeeperElectionSwitch()) {
            boolean hasTryStartZookeeper = false;
            while (true) {
                try {
                    zkElectionLeader.startElection();
                    break;
                } catch (Exception e) {
                    if (hasTryStartZookeeper) {
                        logger.error("Catch an exception when start election,maybe there is something wrong about zookeeper service");
                        throw e;
                    } else {
                        logger.warn("Catch an exception when start election,maybe zookeeper service is not start,try start zookeeper");
                        try {
                            Process process = Runtime.getRuntime().exec(informationCenterAppConfig.getZooKeeperLauncher());
                            process.waitFor();
                            hasTryStartZookeeper = true;
                            logger.warn("succeed to start zookeeper service!");
                        } catch (Exception e1) {
                            logger.error("failed to start zookeeper service,{}", e1);
                            throw e1;
                        }
                    }
                }
            }
        }

//        // check the "admin" account exist in, there maybe have several
//        // information centers, when deploying,
//        // may recreate accounts, which results in information center inc;
//        AccountStore accountStore = informationCenterImpl.getAccountStore();
//        AccountMetadata account = accountStore.getAccount(Constants.SUPERADMIN_DEFAULT_ACCOUNT_NAME);
//        if (account == null) {
//            logger.warn("now create default account: admin ");
//            try {
//                account = accountStore.createAccount(Constants.SUPERADMIN_DEFAULT_ACCOUNT_NAME,
//                        Constants.SUPERADMIN_DEFAULT_ACCOUNT_PASSWORD, Constants.SUPERADMIN_ACCOUNT_TYPE,
//                        Constants.SUPERADMIN_ACCOUNT_ID);
//            } catch (Exception e) {
//                logger.warn("create admin account failure", e);
//                // maybe other information center has created admin account
//                // successfully;
//                account = accountStore.getAccount(Constants.SUPERADMIN_DEFAULT_ACCOUNT_NAME);
//            }
//
//            if (account == null) {
//                String errMsg = "cannot create default account accountId:" + Constants.SUPERADMIN_ACCOUNT_ID;
//                logger.error(errMsg);
//                throw new Exception(errMsg);
//            }
//        }

        // set the max number threads of information center, if the data node
        // number become bigger, we should enlarge
        // the max number of threads
        super.setMaxNumThreads(100);
        super.setMaxNetworkFrameSize(informationCenterAppConfig.getMaxNetworkFrameSize());
        super.start();

        driverStoreSweeperExecutor.start();
        volumeSweeperExecutor.start();
        timeoutSweeperExecutor.start();
        instanceMetadataStoreSweeperExecutor.start();
        serverNodeAlertCheckerExecutor.start();
        // alarmSweeperExecutor.start();
    }

    public void setZkElectionLeader(ZkElectionLeader zkElectionLeader) {
        this.zkElectionLeader = zkElectionLeader;
    }

    public void setInformationCenterAppConfig(InformationCenterAppConfig informationCenterAppConfig) {
        this.informationCenterAppConfig = informationCenterAppConfig;
    }

    public PeriodicWorkExecutorImpl getVolumeSweeperExecutor() {
        return volumeSweeperExecutor;
    }

    public void setVolumeSweeperExecutor(PeriodicWorkExecutorImpl volumeSweeperExecutor) {
        this.volumeSweeperExecutor = volumeSweeperExecutor;
    }

    public void setTimeoutSweeperExecutor(PeriodicWorkExecutorImpl timeoutSweeperExecutor) {
        this.timeoutSweeperExecutor = timeoutSweeperExecutor;
    }

    public void setInstanceMetadataStoreSweeperExecutor(PeriodicWorkExecutorImpl instanceMetadataStoreSweeperExecutor) {
        this.instanceMetadataStoreSweeperExecutor = instanceMetadataStoreSweeperExecutor;
    }

    public PeriodicWorkExecutorImpl getInstanceMetadataStoreSweeperExecutor() {
        return instanceMetadataStoreSweeperExecutor;
    }

    public PeriodicWorkExecutorImpl getDriverStoreSweeperExecutor() {
        return driverStoreSweeperExecutor;
    }

    public void setDriverStoreSweeperExecutor(PeriodicWorkExecutorImpl driverStoreSweeperExecutor) {
        this.driverStoreSweeperExecutor = driverStoreSweeperExecutor;
    }

    public PeriodicWorkExecutorImpl getAlarmSweeperExecutor() {
        return alarmSweeperExecutor;
    }

    public void setAlarmSweeperExecutor(PeriodicWorkExecutorImpl alarmSweeperExecutor) {
        this.alarmSweeperExecutor = alarmSweeperExecutor;
    }

    public PeriodicWorkExecutorImpl getServerNodeAlertCheckerExecutor() {
        return serverNodeAlertCheckerExecutor;
    }

    public void setServerNodeAlertCheckerExecutor(PeriodicWorkExecutorImpl serverNodeAlertCheckerExecutor) {
        this.serverNodeAlertCheckerExecutor = serverNodeAlertCheckerExecutor;
    }

    public void stop() {
        if (zkElectionLeader != null) {
            zkElectionLeader.close();
        }

        if (driverStoreSweeperExecutor != null) {
            driverStoreSweeperExecutor.stop();
        }

        if (volumeSweeperExecutor != null) {
            volumeSweeperExecutor.stop();
        }

        if (timeoutSweeperExecutor != null) {
            timeoutSweeperExecutor.stop();
        }

        if (instanceMetadataStoreSweeperExecutor != null) {
            instanceMetadataStoreSweeperExecutor.stop();
        }

        if (alarmSweeperExecutor != null) {
            alarmSweeperExecutor.stop();
        }

        if (serverNodeAlertCheckerExecutor != null){
            serverNodeAlertCheckerExecutor.stop();
        }

        super.stop();
    }

}
