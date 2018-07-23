package py.infocenter.worker;

import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import py.app.context.AppContext;
import py.common.PyService;
import py.dih.client.DIHClientFactory;
import py.dih.client.DIHServiceBlockingClientWrapper;
import py.icshare.AlarmInfo;
import py.infocenter.store.AlarmStore;
import py.instance.Instance;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.periodic.Worker;

/**
 * periodically pull syslog from remote machine and save it to {@code AlarmStore}
 * 
 * @author sxl
 * 
 */
public class AlarmSweeper implements Worker {
    private static final Logger logger = LoggerFactory.getLogger(AlarmSweeper.class);
    private AlarmStore alarmStore;
    private AppContext appContext;
    private InstanceStore instanceStore;
    private DIHClientFactory dihClientFactory;

    @Override
    public void doWork() throws Exception {
        logger.debug("Alarm sweeper running");

        // pull data from remote dih service
        synchronized (alarmStore) {

            if (appContext.getStatus() == InstanceStatus.SUSPEND) {
                // delete memory data
                logger.info("++clear all alarms :{}", alarmStore);
                alarmStore.clearMemoryData();
                return;
            }

            Set<Instance> instances = instanceStore.getAll(PyService.DIH.getServiceName());
            logger.debug("All DIH instances are : {}", instances);
            for (Instance instance : instances) {
                logger.debug("Going to generate a client to {}", instance.getEndPoint());
                DIHServiceBlockingClientWrapper clientWrapper = dihClientFactory.build(instance.getEndPoint());
                long lastReportTime = alarmStore.getLastReportTime(instance.getEndPoint());
                List<AlarmInfo> alarmInfos;
                try {
                    alarmInfos = clientWrapper.getSyslog(lastReportTime);
                } catch (TException e) {
                    continue;
                }

                logger.debug("All alarm informations size : {}", alarmInfos.size());
                for (AlarmInfo alarmInfo : alarmInfos) {
                    AlarmInfo alarmInStore = alarmStore.get(instance.getEndPoint(), alarmInfo.getSourceObject());

                    // TODO : it's not elegant to set end-point here. but it's the easiest way
                    alarmInfo.setEndpoint(instance.getEndPoint());
                    logger.debug("Current alarm information : {}", alarmInfo);

                    if (alarmInStore != null) {
                        if (alarmInfo.getOper().equals(AlarmInfo.AlarmOper.APPEAR)) {
                            logger.debug("Alarm appears, going to increase the alarm appearence times");
                            alarmStore.increaseAlarmTimes(alarmInfo);
                        } else if (alarmInfo.getOper().equals(AlarmInfo.AlarmOper.DISAPPEAR)) {
                            logger.debug("Going to delete {} from alarm information store", alarmInfo);
                            alarmStore.delete(alarmInfo);
                        } else {
                            logger.error("Invailid alarm operation");
                            throw new Exception();
                        }
                    } else {
                        logger.debug("Going to save {} to alarm information store", alarmInfo);
                        alarmStore.save(alarmInfo);
                    }
                }
            }

        }
    }

    public AlarmStore getAlarmStore() {
        return alarmStore;
    }

    public void setAlarmStore(AlarmStore alarmStore) {
        this.alarmStore = alarmStore;
    }

    public AppContext getAppContext() {
        return appContext;
    }

    public void setAppContext(AppContext appContext) {
        this.appContext = appContext;
    }

    public InstanceStore getInstanceStore() {
        return instanceStore;
    }

    public void setInstanceStore(InstanceStore instanceStore) {
        this.instanceStore = instanceStore;
    }

    public DIHClientFactory getDihClientFactory() {
        return dihClientFactory;
    }

    public void setDihClientFactory(DIHClientFactory dihClientFactory) {
        this.dihClientFactory = dihClientFactory;
    }

}
