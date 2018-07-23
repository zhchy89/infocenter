package py.infocenter.store;

import java.util.List;

import py.common.struct.EndPoint;
import py.icshare.AlarmInfo;

public interface AlarmStore {

    public void increaseAlarmTimes(AlarmInfo alarmInfo);

    public void save(AlarmInfo alarmInfo);

    public void save(List<AlarmInfo> alarmInfos);

    public AlarmInfo get(EndPoint endPoint, String objectSource);

    public List<AlarmInfo> list();

    public List<AlarmInfo> listActiveAlarm();

    public void delete(AlarmInfo alarmInfo) throws Exception;

    /**
     * get last time-stamp by end-point
     * 
     * @param endPoint
     * @return
     */
    public long getLastReportTime(EndPoint endPoint);

    /**
     * clear all memory map to sync data from database again
     */
    public void clearMemoryData();
}
