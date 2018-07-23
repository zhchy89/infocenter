package py.infocenter.service;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import py.common.RequestIdBuilder;
import py.icshare.*;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.store.AccessRuleStore;
import py.infocenter.store.VolumeRuleRelationshipStore;
import py.infocenter.store.VolumeStore;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.*;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;
import py.volume.snapshot.GenericVolumeSnapshotManager;

/**
 * A class includes some tests for applying volume access rule.
 * 
 * @author zjm
 *
 */
public class ApplyVolumeAccessRuleTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(ApplyVolumeAccessRuleTest.class);

    private InformationCenterImpl icImpl;

    @Mock
    private AccessRuleStore accessRuleStore;

    @Mock
    private VolumeRuleRelationshipStore volumeRuleRelationshipStore;

    @Mock
    private VolumeStore volumeStore;
    @Mock
    private InfoCenterAppContext appContext;

    @Before
    public void init() throws Exception {
        super.init();

        icImpl = new InformationCenterImpl();

        VolumeMetadata volume = new VolumeMetadata();
        volume.setVolumeStatus(VolumeStatus.Available);
        volume.setReadWrite(VolumeMetadata.ReadWriteType.READ_WRITE);
        volume.setVolumeId(1);
        volume.setName("volume_1");
        volume.setVolumeType(VolumeType.SMALL);
        volume.setCacheType(CacheType.NONE);
        volume.setVolumeStatus(VolumeStatus.Available);
        volume.setReadWrite(VolumeMetadata.ReadWriteType.READ_WRITE);
        volume.setSnapshotManager(new GenericVolumeSnapshotManager(1));


        when(volumeStore.getVolume(anyLong())).thenReturn(volume);
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);

        icImpl.setAccessRuleStore(accessRuleStore);
        icImpl.setVolumeRuleRelationshipStore(volumeRuleRelationshipStore);
        icImpl.setVolumeStore(volumeStore);
        icImpl.setAppContext(appContext);
    }

    /**
     * Test for applying volume with false value for "commit" field in request. In the case, no access rules could be
     * applied to volume but status transfers to applying.
     * 
     * @throws Exception
     */
    @Test
    public void testApplyVolumeAccessRulesWithoutCommit() throws Exception {
        ApplyVolumeAccessRulesRequest request = new ApplyVolumeAccessRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(0L);
        request.setCommit(false);
        request.addToRuleIds(0L);
        request.addToRuleIds(1L);
        request.addToRuleIds(2L);
        request.addToRuleIds(3L);
        request.addToRuleIds(4L);

        // the access rule with current status deleting after action "apply" is still deleting
        when(accessRuleStore.get(eq(0L))).thenReturn(buildAccessRuleInformation(0L, AccessRuleStatus.DELETING));
        when(accessRuleStore.get(eq(1L))).thenReturn(buildAccessRuleInformation(1L, AccessRuleStatus.AVAILABLE));
        when(accessRuleStore.get(eq(2L))).thenReturn(buildAccessRuleInformation(2L, AccessRuleStatus.AVAILABLE));
        when(accessRuleStore.get(eq(3L))).thenReturn(buildAccessRuleInformation(3L, AccessRuleStatus.AVAILABLE));
        when(accessRuleStore.get(eq(4L))).thenReturn(buildAccessRuleInformation(4L, AccessRuleStatus.AVAILABLE));

        when(volumeRuleRelationshipStore.getByRuleId(eq(0L))).thenReturn(
                buildRelationshipInfoList(0L, 0L, AccessRuleStatusBindingVolume.FREE));
        when(volumeRuleRelationshipStore.getByRuleId(eq(1L))).thenReturn(
                buildRelationshipInfoList(1L, 0L, AccessRuleStatusBindingVolume.APPLIED));
        when(volumeRuleRelationshipStore.getByRuleId(eq(2L))).thenReturn(
                buildRelationshipInfoList(2L, 0L, AccessRuleStatusBindingVolume.APPLING));
        // the access rule with current status canceling after action "apply" is still canceling
        when(volumeRuleRelationshipStore.getByRuleId(eq(3L))).thenReturn(
                buildRelationshipInfoList(3L, 0L, AccessRuleStatusBindingVolume.CANCELING));
        when(volumeRuleRelationshipStore.getByRuleId(eq(4L))).thenReturn(
                buildRelationshipInfoList(4L, 0L, AccessRuleStatusBindingVolume.FREE));

        ApplyVolumeAccessRulesResponse response = icImpl.applyVolumeAccessRules(request);

        class IsAppling extends ArgumentMatcher<VolumeRuleRelationshipInformation> {
            public boolean matches(Object o) {
                if (o == null) {
                    return false;
                }
                VolumeRuleRelationshipInformation relationshipInfo = (VolumeRuleRelationshipInformation) o;
                // the status of free access rules could be applying after action "apply"
                if (relationshipInfo.getRuleId() != 4l
                        || relationshipInfo.getStatus().equals(AccessRuleStatusBindingVolume.APPLING.name())) {
                    return true;
                } else
                    return false;
            }
        }
        ;

        Mockito.verify(volumeRuleRelationshipStore, Mockito.times(1)).save(argThat(new IsAppling()));
        Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
        for (VolumeAccessRule_Thrift ruleFromRemote: response.getAirAccessRuleList()) {
            long ruleId = ruleFromRemote.getRuleId();
            Assert.assertTrue(ruleId == 0L || ruleId == 3L);
        }
    }

    /**
     * Test for applying volume with true value for "commit" field in request. In the case, all access rules except
     * those who are canceling or deleting could be applied to volume.
     * 
     * @throws Exception
     */
    @Test
    public void testApplyVolumeAccessRulesWithCommit() throws Exception {
        ApplyVolumeAccessRulesRequest request = new ApplyVolumeAccessRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(0L);
        request.setCommit(true);
        request.addToRuleIds(0L);
        request.addToRuleIds(1L);
        request.addToRuleIds(2L);
        request.addToRuleIds(3L);
        request.addToRuleIds(4L);

        // no affect after action "apply" on "deleting" volume access rules
        when(accessRuleStore.get(eq(0L))).thenReturn(buildAccessRuleInformation(0L, AccessRuleStatus.DELETING));
        when(accessRuleStore.get(eq(1L))).thenReturn(buildAccessRuleInformation(1L, AccessRuleStatus.AVAILABLE));
        when(accessRuleStore.get(eq(2L))).thenReturn(buildAccessRuleInformation(2L, AccessRuleStatus.AVAILABLE));
        when(accessRuleStore.get(eq(3L))).thenReturn(buildAccessRuleInformation(3L, AccessRuleStatus.AVAILABLE));
        when(accessRuleStore.get(eq(4L))).thenReturn(buildAccessRuleInformation(4L, AccessRuleStatus.AVAILABLE));

        when(volumeRuleRelationshipStore.getByRuleId(eq(0L))).thenReturn(
                buildRelationshipInfoList(0L, 0L, AccessRuleStatusBindingVolume.FREE));
        when(volumeRuleRelationshipStore.getByRuleId(eq(1L))).thenReturn(
                buildRelationshipInfoList(1L, 0L, AccessRuleStatusBindingVolume.APPLIED));
        when(volumeRuleRelationshipStore.getByRuleId(eq(2L))).thenReturn(
                buildRelationshipInfoList(2L, 0L, AccessRuleStatusBindingVolume.APPLING));
        // no affect after action "apply" on "canceling" volume access rules
        when(volumeRuleRelationshipStore.getByRuleId(eq(3L))).thenReturn(
                buildRelationshipInfoList(3L, 0L, AccessRuleStatusBindingVolume.CANCELING));
        when(volumeRuleRelationshipStore.getByRuleId(eq(4L))).thenReturn(
                buildRelationshipInfoList(4L, 0L, AccessRuleStatusBindingVolume.FREE));
        ApplyVolumeAccessRulesResponse response = icImpl.applyVolumeAccessRules(request);

        class IsApplied extends ArgumentMatcher<VolumeRuleRelationshipInformation> {
            public boolean matches(Object o) {
                if (o == null) {
                    return false;
                }
                VolumeRuleRelationshipInformation relationshipInfo = (VolumeRuleRelationshipInformation) o;
                // "free" and "appling" volume access rules turn into "applied" after action "apply"
                if (relationshipInfo.getRuleId() != 4L || relationshipInfo.getRuleId() != 2L
                        || relationshipInfo.getStatus().equals(AccessRuleStatusBindingVolume.APPLIED.name())) {
                    return true;
                } else
                    return false;
            }
        }

        Mockito.verify(volumeRuleRelationshipStore, Mockito.times(2)).save(argThat(new IsApplied()));
        Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
        for (VolumeAccessRule_Thrift ruleFromRemote: response.getAirAccessRuleList()) {
            long ruleId = ruleFromRemote.getRuleId();
            Assert.assertTrue(ruleId == 0L || ruleId == 3L);
        }
    }

    /**
     * Volume is marked as readOnly, apply an access rule with write permission.
     * Expected: ApplyFailedDueToVolumeIsReadOnlyException_Thrift
     * @throws Exception
     */
    @Test(expected = ApplyFailedDueToVolumeIsReadOnlyException_Thrift.class)
    public void testApplyWriteAccessRuleToReadOnlyVolume() throws Exception {
        // Prepare volume
        VolumeMetadata volume1 = new VolumeMetadata();
        volume1.setVolumeStatus(VolumeStatus.Available);
        volume1.setReadWrite(VolumeMetadata.ReadWriteType.READ_ONLY);
        when(volumeStore.getVolume(anyLong())).thenReturn(volume1);

        // Prepare access rule
        when(accessRuleStore.get(eq(0L))).thenReturn(buildAccessRuleInformation(0L, AccessRuleStatus.FREE));

        // Start test
        ApplyVolumeAccessRulesRequest request = new ApplyVolumeAccessRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(0L);
        request.setCommit(false);
        request.addToRuleIds(0L);
        icImpl.applyVolumeAccessRules(request);
    }


    /**
     * Test for applying volume with true value, all access rules except
     * those who are canceling or deleting could be applied to volume.
     *
     * @throws Exception
     */
    @Test
    public void testApplyVolumeAccessRuleOnVolumes() throws Exception {
        ApplyVolumeAccessRuleOnVolumesRequest request = new ApplyVolumeAccessRuleOnVolumesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setRuleId(0L);
        request.setCommit(true);

        request.addToVolumeIds(0L);
        request.addToVolumeIds(1L);
        request.addToVolumeIds(2L);
        request.addToVolumeIds(3L);
        request.addToVolumeIds(4L);


        List<VolumeRuleRelationshipInformation> relationshipInfoList = new ArrayList<>();

        relationshipInfoList.add(buildRelationshipInfoList(0L, 0L, AccessRuleStatusBindingVolume.FREE).get(0));
        relationshipInfoList.add(buildRelationshipInfoList(0L, 1L, AccessRuleStatusBindingVolume.APPLIED).get(0));
        relationshipInfoList.add(buildRelationshipInfoList(0L, 2L, AccessRuleStatusBindingVolume.APPLING).get(0));
        relationshipInfoList.add(buildRelationshipInfoList(0L, 3L, AccessRuleStatusBindingVolume.CANCELING).get(0));
        relationshipInfoList.add(buildRelationshipInfoList(0L, 4L, AccessRuleStatusBindingVolume.FREE).get(0));


        when(accessRuleStore.get(eq(0L))).thenReturn(buildAccessRuleInformation(0L, AccessRuleStatus.AVAILABLE));

        when(volumeRuleRelationshipStore.getByRuleId(eq(0L))).thenReturn(
                relationshipInfoList);


        ApplyVolumeAccessRuleOnVolumesResponse response = icImpl.applyVolumeAccessRuleOnVolumes(request);

        class IsApplied extends ArgumentMatcher<VolumeRuleRelationshipInformation> {
            public boolean matches(Object o) {
                if (o == null) {
                    return false;
                }
                VolumeRuleRelationshipInformation relationshipInfo = (VolumeRuleRelationshipInformation) o;
                // "free" and "appling" volume access rules turn into "applied" after action "apply"
                if (relationshipInfo.getStatus().equals(AccessRuleStatusBindingVolume.APPLIED.name())) {
                    return true;
                } else
                    return false;
            }
        }

        Mockito.verify(volumeRuleRelationshipStore, Mockito.times(3)).save(argThat(new IsApplied()));
        //canceling
        assert(response.getAirVolumeList().size() == 1);
    }

    /**
     * Test for applying volume with two rules, which has same ip.
     *  2 Rules with same ip, 5 volume in pool, 3 volume applied rule1
     *  apply rule2, 3 volume will be failed
     * @throws Exception
     */
    @Test//(expected=)
    public void testApplyVolumeAccessRuleOnVolumes_with2SameIpRule() throws Exception {
        long oldRuleId = 0;
        long newRuleId = 1;
        long volumeCount = 5;
        long relationshipVolumeCount = 3;
        ApplyVolumeAccessRuleOnVolumesRequest request = new ApplyVolumeAccessRuleOnVolumesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setRuleId(newRuleId);
        request.setCommit(true);

        List<VolumeRuleRelationshipInformation> relationshipInfoList = new ArrayList<>();
        List<VolumeMetadata> volumeMetadataList = new ArrayList<>();
        Set<Long> appliedVolumeIdList = new HashSet<>();
        for (long i = 0; i < volumeCount; i++){
            long volumeId = i;
            VolumeMetadata volume = new VolumeMetadata();
            volume.setVolumeId(volumeId);
            volume.setName("volume_"+i);
            volume.setVolumeType(VolumeType.SMALL);
            volume.setCacheType(CacheType.NONE);
            volume.setVolumeStatus(VolumeStatus.Available);
            volume.setReadWrite(VolumeMetadata.ReadWriteType.READ_WRITE);
            volume.setSnapshotManager(new GenericVolumeSnapshotManager(volumeId));

            volumeMetadataList.add(volume);

            request.addToVolumeIds(i);

            when(volumeStore.getVolume(i)).thenReturn(volume);

            if (i < relationshipVolumeCount){
                relationshipInfoList.add(buildRelationshipInfoList(oldRuleId, i, AccessRuleStatusBindingVolume.FREE).get(0));
                appliedVolumeIdList.add(i);
            }
        }

        when(accessRuleStore.get(oldRuleId)).thenReturn(buildAccessRuleInformation(oldRuleId, AccessRuleStatus.AVAILABLE, "192.168.2.10", 2));
        when(accessRuleStore.get(newRuleId)).thenReturn(buildAccessRuleInformation(newRuleId, AccessRuleStatus.AVAILABLE, "192.168.2.10", 1));
        when(volumeRuleRelationshipStore.getByRuleId(oldRuleId)).thenReturn(relationshipInfoList);

        for (int i = 0; i < relationshipVolumeCount; i++){
            List<VolumeRuleRelationshipInformation> volumeRelationshipInfoList = new ArrayList<>();
            volumeRelationshipInfoList.add(relationshipInfoList.get(i));
            when(volumeRuleRelationshipStore.getByVolumeId(i)).thenReturn(volumeRelationshipInfoList);
        }

        ApplyVolumeAccessRuleOnVolumesResponse response = icImpl.applyVolumeAccessRuleOnVolumes(request);

        assert(response.getAirVolumeList().size() == relationshipVolumeCount);

        Set<Long> failedAppliedVolumeIdList = new HashSet<>();
        for (VolumeMetadata_Thrift volumeMetadata_thrift : response.getAirVolumeList()){
            failedAppliedVolumeIdList.add(volumeMetadata_thrift.getVolumeId());
        }

        assert(appliedVolumeIdList.size() == failedAppliedVolumeIdList.size());
        appliedVolumeIdList.retainAll(failedAppliedVolumeIdList);
        assert(appliedVolumeIdList.size() == failedAppliedVolumeIdList.size());

        Mockito.verify(volumeRuleRelationshipStore, Mockito.times((int)(volumeCount-relationshipVolumeCount))).save(any(VolumeRuleRelationshipInformation.class));
    }

    /**
     * Test for applying volume with two rules, which has different ip.
     *  2 Rules with different ip, 5 volume in pool, 3 volume applied rule1
     *  apply rule2, all volume will be success
     * @throws Exception
     */
    @Test//(expected=)
    public void testApplyVolumeAccessRuleOnVolumes_with2DiffIpRule() throws Exception {
        long oldRuleId = 0;
        long newRuleId = 1;
        long volumeCount = 5;
        long relationshipVolumeCount = 3;
        ApplyVolumeAccessRuleOnVolumesRequest request = new ApplyVolumeAccessRuleOnVolumesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setRuleId(newRuleId);
        request.setCommit(true);

        List<VolumeRuleRelationshipInformation> relationshipInfoList = new ArrayList<>();
        List<VolumeMetadata> volumeMetadataList = new ArrayList<>();
        Set<Long> appliedVolumeIdList = new HashSet<>();
        for (long i = 0; i < volumeCount; i++){
            long volumeId = i;
            VolumeMetadata volume = new VolumeMetadata();
            volume.setVolumeId(volumeId);
            volume.setName("volume_"+i);
            volume.setVolumeType(VolumeType.SMALL);
            volume.setCacheType(CacheType.NONE);
            volume.setVolumeStatus(VolumeStatus.Available);
            volume.setReadWrite(VolumeMetadata.ReadWriteType.READ_WRITE);
            volume.setSnapshotManager(new GenericVolumeSnapshotManager(volumeId));

            volumeMetadataList.add(volume);

            request.addToVolumeIds(i);

            when(volumeStore.getVolume(i)).thenReturn(volume);

            if (i < relationshipVolumeCount){
                relationshipInfoList.add(buildRelationshipInfoList(oldRuleId, i, AccessRuleStatusBindingVolume.FREE).get(0));
                appliedVolumeIdList.add(i);
            }
        }

        when(accessRuleStore.get(oldRuleId)).thenReturn(buildAccessRuleInformation(oldRuleId, AccessRuleStatus.AVAILABLE, "192.168.2.10", 2));
        when(accessRuleStore.get(newRuleId)).thenReturn(buildAccessRuleInformation(newRuleId, AccessRuleStatus.AVAILABLE, "192.168.2.11", 1));
        when(volumeRuleRelationshipStore.getByRuleId(oldRuleId)).thenReturn(relationshipInfoList);

        for (int i = 0; i < relationshipVolumeCount; i++){
            List<VolumeRuleRelationshipInformation> volumeRelationshipInfoList = new ArrayList<>();
            volumeRelationshipInfoList.add(relationshipInfoList.get(i));
            when(volumeRuleRelationshipStore.getByVolumeId(i)).thenReturn(volumeRelationshipInfoList);
        }

        ApplyVolumeAccessRuleOnVolumesResponse response = icImpl.applyVolumeAccessRuleOnVolumes(request);

        assert(response.getAirVolumeList().size() == 0);

        Mockito.verify(volumeRuleRelationshipStore, Mockito.times((int)(volumeCount))).save(any(VolumeRuleRelationshipInformation.class));
    }


    public AccessRuleInformation buildAccessRuleInformation(long ruleId, AccessRuleStatus status) {
        AccessRuleInformation accessRuleInformation = new AccessRuleInformation();
        accessRuleInformation.setIpAddress("");
        accessRuleInformation.setPermission(2);
        accessRuleInformation.setStatus(status.name());
        accessRuleInformation.setRuleId(ruleId);
        return accessRuleInformation;
    }
    public AccessRuleInformation buildAccessRuleInformation(long ruleId, AccessRuleStatus status, String ip, int permission) {
        AccessRuleInformation accessRuleInformation = new AccessRuleInformation();
        accessRuleInformation.setIpAddress(ip);
        accessRuleInformation.setPermission(permission);
        accessRuleInformation.setStatus(status.name());
        accessRuleInformation.setRuleId(ruleId);
        return accessRuleInformation;
    }

    public List<VolumeRuleRelationshipInformation> buildRelationshipInfoList(long ruleId, long volumeId,
            AccessRuleStatusBindingVolume status) {
        VolumeRuleRelationshipInformation relationshipInfo = new VolumeRuleRelationshipInformation();

        relationshipInfo.setRelationshipId(RequestIdBuilder.get());
        relationshipInfo.setRuleId(ruleId);
        relationshipInfo.setVolumeId(volumeId);
        relationshipInfo.setStatus(status.name());

        List<VolumeRuleRelationshipInformation> list = new ArrayList<>();
        list.add(relationshipInfo);
        return list;
    }
}
