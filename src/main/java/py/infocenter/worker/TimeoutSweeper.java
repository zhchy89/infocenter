package py.infocenter.worker;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import py.app.context.AppContext;
import py.icshare.CapacityRecord;
import py.icshare.CapacityRecordStore;
import py.icshare.Domain;
import py.icshare.DomainStore;
import py.icshare.InstanceMetadata;
import py.icshare.Status;
import py.icshare.StoragePool;
import py.icshare.StoragePoolStore;
import py.icshare.TotalAndUsedCapacity;
import py.infocenter.common.InfoCenterConstants;
import py.infocenter.store.SegmentUnitTimeoutStore;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStatusTransitionStore;
import py.infocenter.store.VolumeStore;
import py.instance.InstanceStatus;
import py.periodic.Worker;
import py.utils.Utils;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;

public class TimeoutSweeper implements Worker {
    private static final Logger logger = LoggerFactory.getLogger(TimeoutSweeper.class);
    private VolumeStore volumeStore;
    private VolumeStatusTransitionStore volumeStatusStore;
    private SegmentUnitTimeoutStore segUnitTimeoutStore;
    private long runTimes = 0L;
    private AppContext appContext;
    private Long nextActionTimeIntervalMs;
    private DomainStore domainStore;
    private StoragePoolStore storagePoolStore;
    private CapacityRecordStore capacityRecordStore;
    private StorageStore storageStore;
    private int takeSampleInterValSecond;
    private int storeCapacityRecordCount;
    private long roundTimeInterval;

    public void setAppContext(AppContext appContext) {
        this.appContext = appContext;
    }

    public VolumeStore getVolumeStore() {
        return volumeStore;
    }

    public void setVolumeStore(VolumeStore volumeStore) {
        this.volumeStore = volumeStore;
    }

    public void setSegUnitTimeoutStore(SegmentUnitTimeoutStore segUnitTimeoutStore) {
        this.segUnitTimeoutStore = segUnitTimeoutStore;
    }

    public VolumeStatusTransitionStore getVolumeStatusStore() {
        return volumeStatusStore;
    }

    public void setVolumeStatusStore(VolumeStatusTransitionStore volumeStatusStore) {
        this.volumeStatusStore = volumeStatusStore;
    }

    @Override
    public void doWork() throws Exception {
        if (appContext.getStatus() == InstanceStatus.SUSPEND) {
            // delete the memory database
            logger.info("clear all volume in memory : ");
            segUnitTimeoutStore.clear();
            return;
        }

        // check all the segment unit, to see if some one segment unit is out of data
        runTimes++;
        long now = System.currentTimeMillis();
        List<VolumeMetadata> volumes = volumeStore.listVolumes();

        if (runTimes % 5 == 0) { /* we do not need so hurry to deal with volume status */
            for (VolumeMetadata volume : volumes) {
                // check the volume in toBeCreated status is timeout
                if (volume.getVolumeStatus() == VolumeStatus.ToBeCreated) {
                    if (now - volume.getCreateStartTime() > InfoCenterConstants.getVolumeToBeCreatedTimeout() * 1000L) {
                        /*
                         * Volume in toBeCreated status is timeout and put the volume status transition store
                         */
                        logger.debug(
                                "the volume in toBeCreated status is timeout: created time is {}, current time is {}, timeout is {}",
                                Utils.millsecondToString(volume.getCreateStartTime()), Utils.millsecondToString(now),
                                InfoCenterConstants.getVolumeToBeCreatedTimeout());
                        this.volumeStatusStore.addVolumeToStore(volume);
                    }
                } else if (volume.getVolumeStatus() == VolumeStatus.Dead && volume.getDeadTime() != 0) {
                    // check out the dead volume need to delete in DB
                    // although volume is dead, dead time may be zero. for timeout sweeper may execute between set
                    // volume status and set dead time
                    if (now - volume.getDeadTime() > InfoCenterConstants.getTimeOfdeadVolumeToRemove() * 1000L) {
                        logger.debug(
                                "the volume in dead status is longer than the threshold: deadTime is {}, current time is {}, timeout is {}",
                                Utils.millsecondToString(volume.getDeadTime()), Utils.millsecondToString(now),
                                InfoCenterConstants.getTimeOfdeadVolumeToRemove());
                        this.volumeStatusStore.addVolumeToStore(volume);
                    }
                } else if (volume.getVolumeStatus() == VolumeStatus.Fixing) {
                    // just check if volume fix timeout
                    logger.warn("volume:{} is fixing, should put a task to check if fix timeout", volume.getVolumeId());
                    this.volumeStatusStore.addVolumeToStore(volume);
                }
            }
        }

        // Check if some segment unit is timeout
        Collection<Long> volumeIds = new HashSet<>();
        int count = segUnitTimeoutStore.drainTo(volumeIds);
        if (count != 0) {
            for (Long volumeId : volumeIds) {
                VolumeMetadata volume = volumeStore.getVolume(volumeId);
                if (volume != null) {
                    this.volumeStatusStore.addVolumeToStore(volume);
                    logger.debug("some seg in volume is timeout {}", volume);
                } else {
                    logger.debug("Can not find volume by id {}", volumeId);
                }
            }
        }

        // process domain store
        List<Domain> allDomains = domainStore.listAllDomains();
        for (Domain domain : allDomains) {
            if (domain.getStatus() == Status.Deleting) {
                /*
                 * set twice of nextActionTimeIntervalMs cuz should wait response of report archive
                 */
                if (domain.timePassedLongEnough(nextActionTimeIntervalMs * 2)) {
                    domainStore.deleteDomain(domain.getDomainId());
                }
            }
        }

        // process storage pool store
        List<StoragePool> allStoragePools = storagePoolStore.listAllStoragePools();
        for (StoragePool storagePool : allStoragePools) {
            if (storagePool.getStatus() == Status.Deleting) {
                if (storagePool.timePassedLongEnough(nextActionTimeIntervalMs * 2)) {
                    storagePoolStore.deleteStoragePool(storagePool.getPoolId());
                }
            }
        }

        // when infocenter start, should record or update capacity in (2*(roundTimeInterval/1000), 600S) time
        if (((runTimes > (2 * (roundTimeInterval / 1000))) && (runTimes <= 600))
                || runTimes % takeSampleInterValSecond == 0) {
            Date nowDate = new Date();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
            String dateString = dateFormat.format(nowDate);
            long totalCapacity = 0;
            long freeCapacity = 0;
            for (InstanceMetadata datanode : storageStore.list()) {
                totalCapacity += datanode.getLogicalCapacity();
                freeCapacity += datanode.getFreeSpace();
            }
            long usedCapacity = totalCapacity - freeCapacity;
            TotalAndUsedCapacity capacityInfo = new TotalAndUsedCapacity(totalCapacity, usedCapacity);
            CapacityRecord capacityRecord = capacityRecordStore.getCapacityRecord();
            // after switch another infocenter, still can update
            capacityRecord.addRecord(dateString, capacityInfo);
            // if current record count larger than the number we set, should remove earliest record
            if (capacityRecord.recordCount() > storeCapacityRecordCount) {
                capacityRecord.removeEarliestRecord();
            }
            capacityRecordStore.saveCapacityRecord(capacityRecord);
        }
    }

    public Long getNextActionTimeIntervalMs() {
        return nextActionTimeIntervalMs;
    }

    public void setNextActionTimeIntervalMs(Long nextActionTimeIntervalMs) {
        this.nextActionTimeIntervalMs = nextActionTimeIntervalMs;
    }

    public DomainStore getDomainStore() {
        return domainStore;
    }

    public void setDomainStore(DomainStore domainStore) {
        this.domainStore = domainStore;
    }

    public StoragePoolStore getStoragePoolStore() {
        return storagePoolStore;
    }

    public void setStoragePoolStore(StoragePoolStore storagePoolStore) {
        this.storagePoolStore = storagePoolStore;
    }

    public CapacityRecordStore getCapacityRecordStore() {
        return capacityRecordStore;
    }

    public void setCapacityRecordStore(CapacityRecordStore capacityRecordStore) {
        this.capacityRecordStore = capacityRecordStore;
    }

    public StorageStore getStorageStore() {
        return storageStore;
    }

    public void setStorageStore(StorageStore storageStore) {
        this.storageStore = storageStore;
    }

    public int getTakeSampleInterValSecond() {
        return takeSampleInterValSecond;
    }

    public void setTakeSampleInterValSecond(int takeSampleInterValSecond) {
        this.takeSampleInterValSecond = takeSampleInterValSecond;
    }

    public int getStoreCapacityRecordCount() {
        return storeCapacityRecordCount;
    }

    public void setStoreCapacityRecordCount(int storeCapacityRecordCount) {
        this.storeCapacityRecordCount = storeCapacityRecordCount;
    }

    public long getRoundTimeInterval() {
        return roundTimeInterval;
    }

    public void setRoundTimeInterval(long roundTimeInterval) {
        this.roundTimeInterval = roundTimeInterval;
    }
}
