package py.infocenter.store;

import java.util.List;

import py.driver.DriverMetadata;
import py.driver.DriverType;
import py.thrift.share.AlreadyExistStaticLimitationException_Thrift;
import py.thrift.share.DynamicIOLimitationTimeInterleavingException_Thrift;
import py.icshare.qos.IOLimitation;

public interface DriverStore {
    public List<DriverMetadata> get(long volumeId);

    List<DriverMetadata> getByDriverContainerId(long driverContainerId);

    public List<DriverMetadata> get(long volumeId, int snapshotId);

    public DriverMetadata get(long driverContainerId, long volumeId, DriverType driverType, int snapshotId);

    public List<DriverMetadata> list();

    public void delete(long volumeId);

    // if isAttached is not 0, means this volume has launched

    public void delete(long driverContainerId, long volumeId, DriverType driverType, int snapshotId);

    public void save(DriverMetadata driverMetadata);

    public void clearMemoryData();

    public int updateIOLimit(long driverContainerId, long volumeId, DriverType driverType, int snapshotId,
            IOLimitation ioLimitation) throws AlreadyExistStaticLimitationException_Thrift,
            DynamicIOLimitationTimeInterleavingException_Thrift;

    public int deleteIOLimit(long driverContainerId, long volumeId, DriverType driverType, int snapshotId, long limitId);

    public int changeLimitType(long driverContainerId, long volumeId, DriverType driverType, int snapshotId,
            long limitId, boolean staticLimit);
}