package py.infocenter.worker;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import py.app.context.AppContext;
import py.driver.DriverMetadata;
import py.driver.DriverStateEvent;
import py.driver.DriverStatus;
import py.icshare.DomainStore;
import py.icshare.exception.NotExistedException;
import py.infocenter.store.DriverStore;
import py.instance.InstanceStatus;
import py.monitor.alarmbak.AlarmLevel;
import py.monitor.alarmbak.AlarmMessageData;
import py.monitor.alarmbak.AlarmMessageData.AlarmOper;
import py.monitor.jmx.server.AlarmReporter;
import py.monitor.jmx.server.JmxAgent;
import py.periodic.Worker;

/**
 * If driver container is not available for some reason, the drivers cannot be confirmed in info center. Therefore it is
 * necessary to create a worker to sweep the driver table at fixed time in info center to change the driver status
 * {@link DriverStatus} that after sometime has not been reported from driver container to info center.
 */
public class DriverStoreSweeper implements Worker {
    private static final Logger logger = LoggerFactory.getLogger(DriverStoreSweeper.class);

    private AppContext appContext;

    private DriverStore driverStore;
    private DomainStore domainStore;
    private int timeToRemove;

    private Map<Integer, DriverStatus> lastDriverStatusMap = new HashMap<Integer, DriverStatus>();

    private long timeout;

    public DriverStore getDriverStore() {
        return driverStore;
    }

    public void setDriverStore(DriverStore driverStore) {
        this.driverStore = driverStore;
    }

    public void setAppContext(AppContext appContext) {
        this.appContext = appContext;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    @Override
    public void doWork() throws Exception {
        synchronized (driverStore) {

            if (appContext.getStatus() == InstanceStatus.SUSPEND) {
                // delete memory data
                logger.warn("++clear all driver:{}", driverStore);
                driverStore.clearMemoryData();
                /**
                 * just put domainStore in this host, use this timer to clear domain memory map
                 */
                domainStore.clearMemoryMap();
                return;
            }

            final long currentTime = System.currentTimeMillis();
            for (DriverMetadata driver : driverStore.list()) {
                try {
                    if (currentTime - driver.getLastReportTime() > timeout) {
                        logger.warn("Timeout for driver container reporting new driver to update old driver {}", driver);
                        driver.setDriverStatus(driver.getDriverStatus().turnToNextStatusOnEvent(DriverStateEvent.REPORTTIMEOUT));
                        driverStore.save(driver);
                    }
                } catch (Exception e) {
                    logger.error("Caught an error:", e);
                }

//                try {
//                    reportAlarm(driver);
//                } catch (NotExistedException e) {
//                    continue;
//                } catch (Exception e) {
//                    logger.error("Caught an exception when report alarms", e);
//                    throw e;
//                }
            }
        }
    }

    private void reportAlarm(DriverMetadata driver) throws NotExistedException, Exception {
        AlarmReporter alarmReporter = JmxAgent.getInstance().getAlarmReporter();
        DriverStatus driverLastStatus = lastDriverStatusMap.get(driver.getProcessId());
        if (driverLastStatus == null) {
            lastDriverStatusMap.put(driver.getProcessId(), driver.getDriverStatus());
            if (driver.getDriverStatus().equals(DriverStatus.UNKNOWN)) {
                AlarmMessageData alarmData = new AlarmMessageData();
                alarmData.setAlarmLevel(AlarmLevel.MAJOR);
                alarmData.setAlarmName("driver-alarm");
                alarmData.setAlarmObject(driver.getHostName() + ":" + driver.getPort());
                alarmData.setOper(AlarmOper.APPEAR);
                alarmData.setAlarmDescription(String.format("Driver on volume %s:%d is in bad status",
                        driver.getHostName(), driver.getPort()));
                alarmReporter.report(alarmData);
            }
            throw new NotExistedException();
        }

        logger.warn("Current driver : {}, current status: {},last status: {}", driver, driver.getDriverStatus(),
                driverLastStatus);
        if (driverLastStatus.equals(driver.getDriverStatus())) {
            // if current status is equal with last status. do not send alarm out.
            // do nothing
        } else {
            // if current status is not equal with last status

            AlarmMessageData alarmData = new AlarmMessageData();
            alarmData.setAlarmLevel(AlarmLevel.MAJOR);
            alarmData.setAlarmName("driver-alarm");
            alarmData.setAlarmObject(driver.getHostName() + ":" + driver.getPort());

            if (driver.getDriverStatus().equals(DriverStatus.UNAVAILABLE)
                    || driver.getDriverStatus().equals(DriverStatus.UNKNOWN)) {
                alarmData.setOper(AlarmOper.APPEAR);
                alarmData.setAlarmDescription(String.format("Driver on volume %s:%d is in bad status",
                        driver.getHostName(), driver.getPort()));
            } else {
                alarmData.setOper(AlarmOper.DISAPPEAR);
            }
            alarmReporter.report(alarmData);
        }

        // save last driver status
        lastDriverStatusMap.put(driver.getProcessId(), driver.getDriverStatus());
    }

    public DomainStore getDomainStore() {
        return domainStore;
    }

    public void setDomainStore(DomainStore domainStore) {
        this.domainStore = domainStore;
    }

}
