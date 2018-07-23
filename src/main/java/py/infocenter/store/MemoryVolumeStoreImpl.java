package py.infocenter.store;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;

import py.metrics.PYMetric;
import py.metrics.PYMetricRegistry;
import py.metrics.PYNullMetric;
import py.monitor.jmx.server.ResourceType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;

public class MemoryVolumeStoreImpl implements VolumeStore {
    private final static Logger logger = LoggerFactory.getLogger(MemoryVolumeStoreImpl.class);

    private Map<Long, VolumeMetadata> volumeTable = new ConcurrentHashMap<Long, VolumeMetadata>();
    private PYMetric volumeCounter = PYNullMetric.defaultNullMetric;

    public MemoryVolumeStoreImpl() {
        PYMetricRegistry metricRegistry = PYMetricRegistry.getMetricRegistry();

        try {
            volumeCounter = metricRegistry
                    .registerInstance(PYMetricRegistry.nameEx("MemoryVolumeStoreImpl", "volumeCounter")
                            .type(ResourceType.VOLUME).id("volume counter").build(), new Gauge<Integer>() {

                                @Override
                                public Integer getValue() {
                                    return volumeTable.size();
                                }

                            });
        } catch (Exception e) {
            logger.error("Caught an exception when create metrics", e);
        }
    }

    @Override
    public void saveVolume(VolumeMetadata volumeMetadata) {
        volumeTable.put(volumeMetadata.getVolumeId(), volumeMetadata);
    }

    @Override
    public void deleteVolume(VolumeMetadata volumeMetadata) {
        volumeTable.remove(volumeMetadata.getVolumeId());
    }

    @Override
    public List<VolumeMetadata> listRootVolumes() {
        return internalListVolumes(true);
    }

    @Override
    public List<VolumeMetadata> listVolumes() {
        return internalListVolumes(false);
    }

    private List<VolumeMetadata> internalListVolumes(boolean rootOnly) {
        List<VolumeMetadata> volumes = new ArrayList<>();
        for (VolumeMetadata volume : volumeTable.values()) {
            if (!rootOnly || volume.isRoot()) {
                volumes.add(volume);
            }
        }
        return volumes;
    }

    @Override
    public VolumeMetadata getVolume(Long volumeId) {
        VolumeMetadata volume = volumeTable.get(volumeId);
        return volume;
    }

    @Override
    public void clearData() {
        volumeTable.clear();
    }

    @Override
    public List<VolumeMetadata> listVolumesFromRoot(long rootId) {
        List<VolumeMetadata> volumeMetadatas = new ArrayList<>();
        Long childId = rootId;
        VolumeMetadata child;
        // TODO: childId is an Object, null means child doesn't exist, but value
        // get from DB equals zero meaning the child doesn't
        while (childId != null && childId != 0) {
            child = volumeTable.get(childId);

            if (child == null) {
                logger.error("there is no volume with id {}", childId);
                volumeMetadatas.clear();
                break;
            }

            volumeMetadatas.add(child);
            childId = child.getChildVolumeId();
        }

        return volumeMetadatas;
    }

    @Override
    public List<VolumeMetadata> listVolumesWithSameRoot(long rootId) {
        List<VolumeMetadata> volumesWithSameRoot = new ArrayList<>();
        for (VolumeMetadata volumeMetadata : volumeTable.values()) {
            if (volumeMetadata.getRootVolumeId() == rootId) {
                volumesWithSameRoot.add(volumeMetadata);
            }
        }
        return volumesWithSameRoot;
    }

    @Override
    public int updateExtendingSize(long volumeId, long extendingSize) {
        VolumeMetadata volume = volumeTable.get(volumeId);
        volume.setExtendingSize(extendingSize);
        return 1;// there must be only one volume extendingSize been changed
    }

    @Override
    public int updateDeadTime(long volumeId, long deadTime) {
        VolumeMetadata volume = volumeTable.get(volumeId);
        volume.setDeadTime(deadTime);
        return 1;// there must be only one volume status been changed
    }

    @Override
    public int updateStatusAndVolumeInAction(long volumeId, String status, String volumeInAction) {
        VolumeMetadata volume = volumeTable.get(volumeId);
        volume.setVolumeStatus(VolumeStatus.valueOf(status));
        if (!volumeInAction.equals("")) {
            volume.setInAction(VolumeMetadata.VolumeInAction.valueOf(volumeInAction));
        }
        return 1;// there must be only one volume status been changed
    }

    @Override
    public int updatePersistedItems(long volumeId, long volumeSize, String volumeName, String volumeType, String layout,
            boolean simpleConfigured, int segmentNumToCreateEachTime, long domainId, long storagePoolId,
            Date volumeCreatedTime, Date lastExtendedTime, String volumeSource, String readOnly) {
        throw new NotImplementedException("updateSizeNameTypeToDB");
    }

    @Override
    public VolumeMetadata getVolumeNotDeadByName(String name) {
        for (VolumeMetadata volumeMetadata : volumeTable.values()) {
            if (volumeMetadata.getName().equals(name) && VolumeStatus.Dead != volumeMetadata.getVolumeStatus()) {
                return volumeMetadata;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        String tmp = "";
        for (VolumeMetadata volumeMetadata : volumeTable.values()) {
            tmp += volumeMetadata.getName();
            tmp += "\n";
        }
        return tmp;
    }

    @Override
    public void loadVolumeFromDbToMemory() {
    }

    @Override
    public void loadVolumeInDb() {

    }

    @Override
    public String updateVolumeLayout(long volumeId, int startSegmentIndex, int newSegmentsCount, String volumeLayout) {
        VolumeMetadata volume = volumeTable.get(volumeId);
        if (volumeLayout != null) {
            volume.setVolumeLayout(volumeLayout);
            return volumeLayout;
        }
        logger.warn("now to update volume layout from {} to {}", startSegmentIndex,
                startSegmentIndex + newSegmentsCount);
        for (int index = startSegmentIndex; index < startSegmentIndex + newSegmentsCount; index++) {
            volume.updateVolumeLayout(index, true);
        }
        return volume.getVolumeLayout();
    }

}
