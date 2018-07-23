package py.infocenter.store;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import py.common.struct.EndPoint;
import py.icshare.AlarmInfo;
import py.utils.Utils;

public class AlarmStoreMemoryImpl implements AlarmStore {
    private static final Logger logger = LoggerFactory.getLogger(AlarmStoreMemoryImpl.class);
    private List<AlarmInfo> alarmInfos;

    public AlarmStoreMemoryImpl() {
        alarmInfos = new ArrayList<AlarmInfo>();
    }

    @Override
    public void save(AlarmInfo alarmInfo) {
        logger.debug("Save {} to alarm store", alarmInfo);
        alarmInfos.add(alarmInfo);
    }

    @Override
    public List<AlarmInfo> list() {
        logger.debug("Going to list all alarms from alarm store");
        return alarmInfos;
    }

    @Override
    public synchronized void clearMemoryData() {
        logger.debug("Going to clear alarm store");
        alarmInfos.clear();
    }

    @Override
    public synchronized long getLastReportTime(EndPoint endPoint) {
        logger.debug("Going to get last alarm report time of {}", endPoint);
        long lastReportTime = 0l;
        for (AlarmInfo alarmInfo : alarmInfos) {
            logger.debug("All alarm informations are : {}", alarmInfos);
            if (alarmInfo.getEndpoint().equals(endPoint)) {
                if (lastReportTime < alarmInfo.getTimeStamp()) {
                    lastReportTime = alarmInfo.getTimeStamp();
                }
            }
        }

        logger.debug("Last alarm report time is : {}, format in date : {}", lastReportTime,
                        Utils.millsecondToString(lastReportTime));
        return lastReportTime;
    }

    @Override
    public synchronized void increaseAlarmTimes(AlarmInfo alarmInfo) {
        logger.debug("Before increase alarm counter, the alarm information is : {}", alarmInfo);
        for (AlarmInfo tmp : alarmInfos) {
            if (alarmInfo.equals(tmp)) {
                tmp.setTimes(tmp.getTimes() + 1);
            }
        }
        logger.debug("After increase alarm counter, the alarm information is : {}", alarmInfo);
    }

    @Override
    public synchronized void save(List<AlarmInfo> alarmInfos) {
        logger.debug("Going to add plenty of alarm informations to alarm store");
        alarmInfos.addAll(alarmInfos);
    }

    @Override
    public synchronized AlarmInfo get(EndPoint endPoint, String objectSource) {
        logger.debug("Going to get alarm information from store by condition of {}:{}", endPoint, objectSource);
        for (AlarmInfo alarmInfo : alarmInfos) {
            if (alarmInfo.getEndpoint().equals(endPoint) && alarmInfo.getSourceObject().equals(objectSource)) {
                return alarmInfo;
            }
        }
//        logger.warn("There is no alarm in store on end-point : {}, device : {}", endPoint, objectSource);
        return null;
    }

    @Override
    public synchronized List<AlarmInfo> listActiveAlarm() {
        logger.debug("Going to get all active alarms");
        List<AlarmInfo> filtedAlarms = new ArrayList<AlarmInfo>();
        logger.debug("All alarm infomations are : {}", alarmInfos);
        for (AlarmInfo tmpAlarm : alarmInfos) {
            logger.debug("Current alarm information is : {}", tmpAlarm);
            if (tmpAlarm.getTimes() > 0) {
                filtedAlarms.add(tmpAlarm);
            }
        }

        logger.debug("Filted alarms are : {}", filtedAlarms);
        return filtedAlarms;
    }

    @Override
    public synchronized void delete(AlarmInfo alarmInfo) throws Exception {
        logger.debug("Going to remove alarm infomation {} from alarm store", alarmInfo);
        alarmInfos.remove(alarmInfo);
    }

}
