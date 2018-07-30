package py.infocenter.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import py.app.context.AppContext;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.common.VolumeMetadataJSONParser;
import py.common.client.RequestResponseHelper;
import py.common.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.icshare.CloneRelationshipInformation;
import py.icshare.StoragePoolStore;
import py.infocenter.store.CloneRelationshipsDBStore;
import py.infocenter.store.VolumeStatusTransitionStore;
import py.infocenter.store.VolumeStore;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.periodic.Worker;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.UpdateSegmentUnitVolumeMetadataJsonRequest;
import py.thrift.datanode.service.UpdateSegmentUnitVolumeMetadataJsonResponse;
import py.utils.Utils;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.snapshot.SnapshotMetadata;

import static py.infocenter.service.InformationCenterImpl.mergeVolumes;
import static py.volume.VolumeMetadata.VolumeInAction.NULL;
import static py.volume.VolumeStatus.Available;

/**
 * This worker iterate the volume status transition store and process volume
 * status one by one;
 */
public class VolumeSweeper implements Worker {
    private static final Logger logger = LoggerFactory.getLogger(VolumeSweeper.class);
    private VolumeStore volumeStore;
    private VolumeStatusTransitionStore volumeStatusStore;
    private GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory;
    private int timeout;
    private AppContext appContext;
    private InstanceStore instanceStore;
    public long deadTimeToRemove;
    private StoragePoolStore storagePoolStore;
    private CloneRelationshipsDBStore cloneRelationshipsStore;
    private int updateVolumesStatusTrigger = 60;

    public void doWork() throws Exception {
        if (appContext.getStatus() == InstanceStatus.SUSPEND) {
            // delete the memory database
            logger.info("clear all volume in memory : ");
            volumeStore.clearData();
            volumeStatusStore.clear();
            return;
        }

        /**
         * This if block code is to handle following situation, no need to run every time, indeed only need to run once
         * when infocenter startup, but it cannot be done elsewhere, so we put here and add a trigger to invoke it.
         *
         * Situation: when all the services are shutdown, database has volumes information, this information will update
         * to infocenter when infocenter becomes active. Volumes' status are available in database but datanodes are not
         * active and cannot provide store service, so here we should update volumes' status from available to
         * unavailable first. When all the datanodes are active and volumes will become available again.
         */
        if (1 == (updateVolumesStatusTrigger++ / 60)) {
            List<VolumeMetadata> virtualVolumes = volumeStore.listVolumes();
            for (VolumeMetadata volume : virtualVolumes) {

                /***** when move online ok, set the volume action just for reboot *****/
                boolean cloneStatusForMoveOnline = checkCloneStatusForMoveOnline(volume);
                logger.info("get the status :{}, for volume :{}", cloneStatusForMoveOnline, volume.getVolumeId());

                VolumeStatus volumeStatus = volume.getVolumeStatus();
                VolumeStatus newVolumeStatus = volume.updateStatus(cloneStatusForMoveOnline);
                // if volume status changed, we save status to memory and DB
                if (!newVolumeStatus.equals(volumeStatus)) {
                    volume.setVolumeStatus(newVolumeStatus);
                    // update status to DB
                    volumeStore.updateStatusAndVolumeInAction(volume.getVolumeId(), newVolumeStatus.toString(),
                            volume.getInAction().name());
                }

                if ((volumeStatus != VolumeStatus.Dead) && (newVolumeStatus == VolumeStatus.Dead)) {
                    // once volume status is dead, we should update field of
                    // deadtime in volumes table immediately
                    // otherwise, memory is updated, DB is not, when we compare
                    // memory data with memory data,
                    // they are the same all the time, leading to no update in
                    // DB
                    long deadtime = System.currentTimeMillis();
                    logger.debug("update deadtime to memory and table volumes, volumeId {}, deadtime {} ",
                            volume.getVolumeId(), Utils.millsecondToString(deadtime));
                    // only here can set deadTime, other place should keep this
                    // value no change
                    volume.setDeadTime(deadtime);

                    // update dead time to DB
                    volumeStore.updateDeadTime(volume.getVolumeId(), deadtime);
                }
            }
            updateVolumesStatusTrigger = 0;
        }

        checkCloneFlagForSnapshot();

        ArrayList<VolumeMetadata> volumes = new ArrayList<>();
        int count = volumeStatusStore.drainTo(volumes);

        if (count > 0) { // the queue has volume needs to deal with;
            for (VolumeMetadata volume : volumes) {
                logger.debug("the volume status may be changed: {}", volume);

                VolumeStatus volumeStatus = volume.getVolumeStatus();
                // Volume in dead status, if this status last 6 months, delete
                // volume in the DB;
                if (volumeStatus == VolumeStatus.Dead) {
                    long currentTime = System.currentTimeMillis();
                    if ((volume.getDeadTime() + deadTimeToRemove * 1000L) < currentTime) {
                        logger.warn(
                                "volume:{} is dead, and we can delete volume DEAD TIME:{}, CURRENT TIME:{} , deadTimeToRemove:{}",
                                volume.getVolumeId(), Utils.millsecondToString(volume.getDeadTime()),
                                Utils.millsecondToString(currentTime), deadTimeToRemove);
                        volumeStore.deleteVolume(volume);
                        storagePoolStore.deleteVolumeId(volume.getStoragePoolId(), volume.getVolumeId());
                    }
                    continue;
                }

                /***** when move online ok, set the volume action just for reboot *****/
                boolean cloneStatusForMoveOnline = checkCloneStatusForMoveOnline(volume);
                logger.info("get the status :{}, for volume :{}", cloneStatusForMoveOnline, volume.getVolumeId());

                // get the latest VolumeStatus
                VolumeStatus newVolumeStatus = volume.updateStatus(cloneStatusForMoveOnline);
                // if volume status changed, we save status to memory and DB
                if (!newVolumeStatus.equals(volumeStatus)) {
                    volume.setVolumeStatus(newVolumeStatus);
                    // update status to DB
                    volumeStore.updateStatusAndVolumeInAction(volume.getVolumeId(), newVolumeStatus.toString(),
                            volume.getInAction().name());
                }

                if ((volumeStatus != VolumeStatus.Dead) && (newVolumeStatus == VolumeStatus.Dead)) {
                    // once volume status is dead, we should update field of
                    // deadtime in volumes table immediately
                    // otherwise, memory is updated, DB is not, when we compare
                    // memory data with memory data,
                    // they are the same all the time, leading to no update in
                    // DB
                    long deadtime = System.currentTimeMillis();
                    logger.debug("update deadtime to memory and table volumes, volumeId {}, deadtime {} ",
                            volume.getVolumeId(), Utils.millsecondToString(deadtime));
                    // only here can set deadTime, other place should keep this
                    // value no change
                    volume.setDeadTime(deadtime);

                    // update dead time to DB
                    volumeStore.updateDeadTime(volume.getVolumeId(), deadtime);

                    if (!volume.isRoot()) {
                        long rootId = volume.getRootVolumeId();
                        List<VolumeMetadata> volumeList = volumeStore.listVolumesFromRoot(rootId);
                        VolumeMetadata root = volumeList.get(0);

                        synchronized (root) {
                            long extendingSizeInRoot = root.getExtendingSize();
                            if (extendingSizeInRoot < volume.getVolumeSize()) {
                                logger.warn("extending size " + extendingSizeInRoot
                                        + " is less than the size of the volume that has been extended."
                                        + "This situation could occur when the volume database is lost and extendingSize is NOT stored in datanodes"
                                        + "Do nothing about it.");
                                root.setExtendingSize(0);
                                root.setInAction(NULL);
                            } else if (extendingSizeInRoot == volume.getVolumeSize()) {
                                root.setExtendingSize(0);
                                root.setInAction(NULL);
                            } else {
                                root.setExtendingSize(extendingSizeInRoot - volume.getVolumeSize());
                            }

                            // save the changes of extending size
                            try {
                                volumeStore.updateExtendingSize(root.getVolumeId(), root.getExtendingSize());
                                volumeStore.updateStatusAndVolumeInAction(root.getVolumeId(), root.getVolumeStatus().name(),
                                        root.getInAction().name());
                            } catch (Exception e) {
                                logger.error(
                                        "Caught an exception when updating volume metadata to the database. Reset everything: root volume {}",
                                        root, e);
                                root.setExtendingSize(extendingSizeInRoot);
                                return;
                            }
                        }

                    }

                    continue;
                }

                // we meet in lazy clone situation, source volume is being cloned when target volume is completed
                if (newVolumeStatus == VolumeStatus.Available &&  volume.getInAction() == VolumeMetadata.VolumeInAction.BEING_CLONED) {
                    logger.warn("there is one volume is done with clone, root volume {} action reset to null.",
                            volume.getVolumeId());

                    volumeStore.updateStatusAndVolumeInAction(volume.getVolumeId(),
                            volume.getVolumeStatus().name(), NULL.name());
                }

                if (volume.isRoot()) {
                    //delete cloneRelationship from cloneRelationshipsStore after clone
                    //whatever the clone operation is successful or failure
                    if (volume.getVolumeSource().equals(VolumeMetadata.VolumeSourceType.CLONE_VOLUME)
                            || volume.getVolumeSource().equals(VolumeMetadata.VolumeSourceType.MOVE_VOLUME)
                            || volume.getVolumeSource().equals(VolumeMetadata.VolumeSourceType.MOVE_ONLINE_VOLUME)) {
                        long rootId = volume.getRootVolumeId();
                        List<VolumeMetadata> volumesWithSameRoot = volumeStore.listVolumesWithSameRoot(rootId);

                        boolean cloneRelationshipNeedDelete = true;
                        for (VolumeMetadata volumeToCheckStatus : volumesWithSameRoot) {
                            VolumeStatus volumeCheckStatus = volumeToCheckStatus.getVolumeStatus();
                            //when the cloneVolumeStatus is ToBeCreated or Creating,
                            //we don't delete cloneRelationship from cloneRelationshipsStore
                            if (VolumeStatus.ToBeCreated == volumeCheckStatus
                                    || VolumeStatus.Creating == volumeCheckStatus) {
                                cloneRelationshipNeedDelete = false;
                                break;
                            }

                        }

                        if (cloneRelationshipNeedDelete) {

                            long cloneVolumeId = volume.getVolumeId();
                            CloneRelationshipInformation cloneRelationshipInformation =
                                    cloneRelationshipsStore.getFromDB(cloneVolumeId);

                            if (cloneRelationshipInformation != null) {
                                logger.warn("{} volume is done with clone or move, delete its clone relationship", cloneVolumeId);
                                cloneRelationshipsStore.deleteFromDB(cloneVolumeId);
                                long srcVolumeId = cloneRelationshipInformation.getScrVolumeId();

                                if (cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).isEmpty()) {

                                    VolumeMetadata srcVolumeMetadata = volumeStore.getVolume(srcVolumeId);
                                    VolumeMetadata.VolumeInAction oldInAction = srcVolumeMetadata.getInAction();

                                    if (oldInAction.equals(VolumeMetadata.VolumeInAction.BEING_CLONED) ||
                                            oldInAction.equals(VolumeMetadata.VolumeInAction.BEING_MOVED)) {

                                        logger.warn("there is one volume is done with clone or move, root volume {} action reset to null.",
                                                srcVolumeId);

                                        volumeStore.updateStatusAndVolumeInAction(srcVolumeId,
                                                srcVolumeMetadata.getVolumeStatus().name(), NULL.name());
                                    }
                                }
                            }

                        }

                    }
                } else {
                    linkToRoot(volume);
                }

                if (volume.isNeedToPersistVolumeLayout()) {
                    logger.warn("now to persist modified volume layout to data node");
                    updateVolumeMetadataToDataNode(volume);
                }
            } // for volume list
        }
    }

    private void linkToRoot(VolumeMetadata volumeMetadata) {
        long rootId = volumeMetadata.getRootVolumeId();
        long myId = volumeMetadata.getVolumeId();

        List<VolumeMetadata> volumes = volumeStore.listVolumesFromRoot(rootId);
        // current there may be no volume in the root-child relationship chain.
        // it maybe occur when info center reboot
        if (volumes.size() == 0) {
            return;
        }

        if (volumeMetadata.getVolumeStatus() != Available) {
            return;
        }

        // check root volume is in deleting, deleted or dead status, if so, it
        // also need to delete child volume
        VolumeMetadata root = volumes.get(0);
        if (root.isDeletedByUser()) {
            // set the volume deleting, next time data node report segment unit
            // to info center, info center will notify data node to delete
            // segment unit
            volumeMetadata.setVolumeStatus(VolumeStatus.Deleting);
        }

        if (volumeMetadata.isUpdatedToDataNode()) {
            logger.debug("volume {} has been updated to the data node. Do nothing", myId);
            Validate.isTrue(volumeMetadata.isPersistedToDatabase());
            return;
        }

        VolumeMetadata prev = root;
        for (VolumeMetadata volume : volumes) {
            if (volume.getVolumeId() == myId) {
                break;
            }
            prev = volume;
        }

        //delete cloneRelationship from cloneRelationshipsStore after clone
        //whatever the clone operation is successful or failure
        if (root.getVolumeSource().equals(VolumeMetadata.VolumeSourceType.CLONE_VOLUME)
                || root.getVolumeSource().equals(VolumeMetadata.VolumeSourceType.MOVE_VOLUME)
                || root.getVolumeSource().equals(VolumeMetadata.VolumeSourceType.MOVE_ONLINE_VOLUME)) {
            List<VolumeMetadata> volumesWithSameRoot = volumeStore.listVolumesWithSameRoot(rootId);

            boolean cloneRelationshipNeedDelete = true;
            for (VolumeMetadata volumeToCheckStatus : volumesWithSameRoot) {
                VolumeStatus volumeCheckStatus = volumeToCheckStatus.getVolumeStatus();
                //when the cloneVolumeStatus is ToBeCreated or Creating,
                //we don't delete cloneRelationship from cloneRelationshipsStore
                if (VolumeStatus.ToBeCreated == volumeCheckStatus
                        || VolumeStatus.Creating == volumeCheckStatus) {
                    cloneRelationshipNeedDelete = false;
                    break;
                }
            }
            if (cloneRelationshipNeedDelete) {
                long cloneVolumeId = root.getVolumeId();
                CloneRelationshipInformation cloneRelationshipInformation =
                        cloneRelationshipsStore.getFromDB(cloneVolumeId);
                if (cloneRelationshipInformation != null) {
                    logger.warn("{} volume is done with clone, delete its clone relationship", cloneVolumeId);
                    cloneRelationshipsStore.deleteFromDB(cloneVolumeId);

                    long srcVolumeId = cloneRelationshipInformation.getScrVolumeId();
                    if (cloneRelationshipsStore.listBySrcVolumeId(srcVolumeId).isEmpty()) {
                        VolumeMetadata srcVolumeMetadata = volumeStore.getVolume(srcVolumeId);
                        VolumeMetadata.VolumeInAction oldInAction = srcVolumeMetadata.getInAction();
                        if (oldInAction.equals(VolumeMetadata.VolumeInAction.BEING_CLONED) ||
                                oldInAction.equals(VolumeMetadata.VolumeInAction.BEING_MOVED)) {
                            logger.warn("there is one volume is done with clone or move, root volume {} action reset to null.",
                                    srcVolumeId);
                            volumeStore.updateStatusAndVolumeInAction(srcVolumeId,
                                    srcVolumeMetadata.getVolumeStatus().name(), NULL.name());
                        }
                    }
                }
            }
        }

        // volume is not updated to the data node
        if (!volumeMetadata.isPersistedToDatabase()) {
            // volume has NOT persisted to DB yet
            logger.debug("Find the previous volume {} in the volume group starting from {} for volume {}", prev, rootId,
                    myId);
            if (prev.getChildVolumeId() != null) {
                logger.warn(
                        "The last volume 's child id is not null. This might be possible when info center is restarted and saving volumes failed");
                Validate.isTrue(myId == prev.getChildVolumeId(),
                        myId + " should be equal to " + prev.getChildVolumeId());
            }

            // don't change the order of saving volumes
            synchronized (prev) {
                Long oldChildId = prev.getChildVolumeId();
                prev.setChildVolumeId(myId);
                prev.incVersion();
                try {
                    volumeStore.saveVolume(prev);
                } catch (Exception e) {
                    logger.error("Caught an exception when updating volume metadata to the database. Reset everything"
                            + prev, e);
                    prev.setChildVolumeId(oldChildId);
                    prev.decVersion();
                    return;
                }
            }

            synchronized (volumeMetadata) {
                int positionOfFirstSegInLogicVolume = volumeMetadata.getPositionOfFirstSegmentInLogicVolume();
                int newPositionOfFirstSegInLogicVolume =
                        prev.getPositionOfFirstSegmentInLogicVolume() + prev.getSegmentCount();
                volumeMetadata.setPositionOfFirstSegmentInLogicVolume(newPositionOfFirstSegInLogicVolume);
                volumeMetadata.incVersion();
                try {
                    volumeStore.saveVolume(volumeMetadata);
                } catch (Exception e) {
                    logger.error(
                            "Caught an exception when updating volume metadata to the database. Reset everything: {}",
                            volumeMetadata, e);
                    volumeMetadata.setPositionOfFirstSegmentInLogicVolume(positionOfFirstSegInLogicVolume);
                    volumeMetadata.decVersion();
                    return;
                }
            }

            synchronized (root) {
                long extendingSizeInRoot = root.getExtendingSize();
                if (extendingSizeInRoot < volumeMetadata.getVolumeSize()) {
                    logger.warn("extending size " + extendingSizeInRoot
                            + " is less than the size of the volume that has been extended."
                            + "This situation could occur when the volume database is lost and extendingSize is NOT stored in datanodes"
                            + "Do nothing about it.");
                    root.setExtendingSize(0);
                    root.setInAction(NULL);
                } else if (extendingSizeInRoot == volumeMetadata.getVolumeSize()) {
                    root.setExtendingSize(0);
                    root.setInAction(NULL);
                } else {
                    root.setExtendingSize(extendingSizeInRoot - volumeMetadata.getVolumeSize());
                }

                // save the changes of extending size
                try {
                    volumeStore.updateExtendingSize(root.getVolumeId(), root.getExtendingSize());
                    volumeStore.updateStatusAndVolumeInAction(root.getVolumeId(), root.getVolumeStatus().name(),
                            root.getInAction().name());
                } catch (Exception e) {
                    logger.error(
                            "Caught an exception when updating volume metadata to the database. Reset everything: root volume {}",
                            root, e);
                    root.setExtendingSize(extendingSizeInRoot);
                    return;
                }
            }
        }

        volumeMetadata.setPersistedToDatabase(true);
        // At this point, the volume has been saved to the database but not yet
        // updated to datanodes
        // let's update segment unit metadata in data node
        if (updateVolumeMetadataToDataNode(prev) && updateVolumeMetadataToDataNode(volumeMetadata)) {
            volumeMetadata.setUpdatedToDataNode(true);
        }
    }

    /**
     * @param volumeMetadata
     * @return true: success false: fail
     * this function update the volume meta data to data node where the
     * primary instance of first segment exists
     */
    private boolean updateVolumeMetadataToDataNode(VolumeMetadata volumeMetadata) {
        if (volumeMetadata.getSegmentTableSize() == 0) {
            logger.warn("no segmenttable in volumemedata {} ", volumeMetadata);
            return false;
        }

        // Get the primary instance of first segment. And update volume meta
        // data to data node
        SegmentMetadata firstSegmentMetadata = volumeMetadata.getSegmentByIndex(0);
        if (firstSegmentMetadata == null) {
            logger.warn("no segment indexed 0 in {} ", volumeMetadata);
            return false;
        }

        InstanceId primary = null;
        for (Entry<InstanceId, SegmentUnitMetadata> entry : firstSegmentMetadata.getSegmentUnitMetadataTable()
                .entrySet()) {
            if (entry.getKey().equals(entry.getValue().getMembership().getPrimary())) {
                primary = entry.getKey();
                break;
            }
        }
        if (primary == null) {
            logger.warn(
                    "can't update the new volume metadata because can't find the primary of the first segment unit. The primary is null");
            return false;
        }

        // build volumemetadata as a string
        ObjectMapper mapper = new ObjectMapper();
        String volumeString = null;
        try {
            volumeString = mapper.writeValueAsString(volumeMetadata);
        } catch (JsonProcessingException e) {
            logger.error("failed to build volumemetadata string ", e);
            return false;
        }

        VolumeMetadataJSONParser parser = new VolumeMetadataJSONParser(volumeMetadata.getVersion(), volumeString);
        UpdateSegmentUnitVolumeMetadataJsonRequest request = RequestResponseHelper
                .buildUpdateSegmentUnitVolumeMetadataJsonRequest(
                        new SegId(firstSegmentMetadata.getVolume().getVolumeId(), firstSegmentMetadata.getIndex()),
                        firstSegmentMetadata.getSegmentUnitMetadata(primary).getMembership(),
                        parser.getCompositedVolumeMetadataJSON());

        EndPoint primaryEndpoint = null;
        try {
            primaryEndpoint = instanceStore.get(primary).getEndPoint();
            if (primaryEndpoint == null) {
                logger.warn("can't update the new volume metadata because can't find the primary endpoint");
                return false;
            }

            DataNodeService.Iface dataNodeClient = dataNodeClientFactory.generateSyncClient(primaryEndpoint, timeout);
            UpdateSegmentUnitVolumeMetadataJsonResponse response = dataNodeClient
                    .updateSegmentUnitVolumeMetadataJson(request);
            if (response != null) {
                logger.debug("successful updated segmetadata");
                return true;
            }

        } catch (Exception e) {
            logger.warn("write volume metadata json to primary: {}", primaryEndpoint, e);
        }
        return false;
    }

    public StoragePoolStore getStoragePoolStore() {
        return storagePoolStore;
    }

    public void setStoragePoolStore(StoragePoolStore storagePoolStore) {
        this.storagePoolStore = storagePoolStore;
    }

    public CloneRelationshipsDBStore getCloneRelationshipsStore() {
        return cloneRelationshipsStore;
    }

    public void setCloneRelationshipsStore(CloneRelationshipsDBStore cloneRelationshipsStore) {
        this.cloneRelationshipsStore = cloneRelationshipsStore;
    }

    public long getDeadTimeToRemove() {
        return deadTimeToRemove;
    }

    public void setDeadTimeToRemove(long deadTimeToRemove) {
        this.deadTimeToRemove = deadTimeToRemove;
    }

    public void setVolumeStore(VolumeStore volumeStore) {
        this.volumeStore = volumeStore;
    }

    public void setVolumeStatusTransitionStore(VolumeStatusTransitionStore store) {
        this.volumeStatusStore = store;
    }

    public void setAppContext(AppContext appContext) {
        this.appContext = appContext;
    }

    public void setDataNodeClientFactory(GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory) {
        this.dataNodeClientFactory = dataNodeClientFactory;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public void setInstanceStore(InstanceStore instanceStore) {
        this.instanceStore = instanceStore;
    }

    private boolean checkCloneStatusForMoveOnline(VolumeMetadata volumeMetadata) {
        /** just for move online, get the new volume clone status **/
        if (volumeMetadata.getVolumeSource() != VolumeMetadata.VolumeSourceType.MOVE_ONLINE_VOLUME) {
            return false;
        }

        /** clone ok **/
        if (volumeMetadata.isCloneStatusWhenMoveOnline()) {
            return true;
        }

        /** only check root volume **/
        if (!volumeMetadata.isRoot()) {
            return false;
        }

        return checkAndChangeVolumeMetadata(volumeMetadata, CheckType.MoveOnLine);

    }

    private void checkCloneFlagForSnapshot() {
        logger.debug("checkCloneFlagForSnapshot...");
        List<VolumeMetadata> volumeMetadatas = volumeStore.listRootVolumes();
        for (VolumeMetadata volumeMetadata : volumeMetadatas) {
            // when clone volume from a snapshot, only add cloningVolumeId and cloningSnapshotId to root volume;
            if (volumeMetadata.isRoot() && volumeMetadata.getCloningSnapshotId() != SnapshotMetadata
                    .ORIGIN_SNAPSHOT_ID && volumeMetadata.getCloningVolumeId() != 0) {
                if (checkAndChangeVolumeMetadata(volumeMetadata, CheckType.CloneSnapshot)) {
                    logger.warn("set cloningVolumeId and cloningSnapshotId to 0 ok, volumeId is: {}", volumeMetadata
                            .getVolumeId());
                } else {
                    logger.warn("not set cloningVolumeId and cloningSnapshotId to 0, volumeId is: {}", volumeMetadata
                            .getVolumeId());
                }
            }
        }
    }

    private boolean checkAndChangeVolumeMetadata(VolumeMetadata volumeMetadata, CheckType checkType){
        long volumeId = volumeMetadata.getVolumeId();
        List<VolumeMetadata> volumeMetadatas = volumeStore.listVolumesFromRoot(volumeId);
        if (volumeMetadatas.size() == 0) {
            logger.error("move online can not get volumeMetadata from volumeId: {}", volumeId);
            return false;
        }

        if (volumeMetadatas.get(0).getVolumeId() != volumeId) {
            logger.error("move online The first volume {} has different volume id {}", volumeMetadatas.get(0), volumeId);
            return false;
        }

        // iterate all segment unit's and change their statuses accordingly
        VolumeMetadata virtualVolumeMetadata = mergeVolumes(volumeMetadatas, true, false, 0, 0);
        if (virtualVolumeMetadata.isStable()) {
            boolean cloneDone = true;
            for (SegmentMetadata segmentMetadata : virtualVolumeMetadata.getSegments()) {
                for (SegmentUnitMetadata segmentUnitMetadata : segmentMetadata.getSegmentUnits()) {
                    if (!segmentUnitMetadata.isArbiter() && Double.compare(segmentUnitMetadata.getRatioClone(), 1.00d) != 0) {
                        cloneDone = false;
                        logger.warn("segment unit {} ratioClone is {}, not clone done.",
                                segmentUnitMetadata, segmentUnitMetadata.getRatioClone());
                        break;
                    }
                }
                if (!cloneDone) {
                    break;
                }
            }

            if (cloneDone) {
                switch (checkType) {
                    case MoveOnLine:
                        /** when clone ok, save the clone ok status **/
                        volumeMetadata.setCloneStatusWhenMoveOnline(true);
                        volumeStore.saveVolume(volumeMetadata);
                        break;
                    case CloneSnapshot:
                        /**
                         * when clone is ok, set cloningVolumeId and cloningSnapshotId to 0;
                         */
                        VolumeMetadata volumeMetadata1 = new VolumeMetadata();
                        volumeMetadata1.deepCopy(volumeMetadata);
                        volumeMetadata1.setCloningVolumeId(0);
                        volumeMetadata1.setCloningSnapshotId(0);
                        volumeStore.saveVolume(volumeMetadata1);
                        break;
                }
                logger.warn("VolumeStable moveOnline, the new volume:{} clone ok ", volumeId);
                return true;
            }
        }
        return false;
    }
}

enum CheckType {
    MoveOnLine,
    CloneSnapshot
}
