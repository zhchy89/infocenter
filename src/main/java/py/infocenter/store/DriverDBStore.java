package py.infocenter.store;

import java.util.List;

import py.driver.DriverType;
import py.icshare.DriverInformation;

public interface DriverDBStore {

    public void updateToDB(DriverInformation driverInformation);

    public void saveToDB(DriverInformation driverInformation);

    public List<DriverInformation> getByVolumeIdFromDB(long volumeId);
    
    public List<DriverInformation> getByDriverKeyFromDB(long volumeId, DriverType driverType, int snapshotId);

    List<DriverInformation> getByDriverContainerIdFromDB(long driverContainerId);

    public List<DriverInformation> listFromDB();

    public int deleteFromDB(long volumeId);

    public int deleteFromDB(long volumeId, DriverType driverType, int snapshotId);

    public int updateStatusToDB(long volumeId, DriverType driverType, int snapshotId, String status);

}
