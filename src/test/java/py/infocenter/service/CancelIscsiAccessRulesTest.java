package py.infocenter.service;

import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.icshare.AccessRuleStatus;
import py.icshare.AccessRuleStatusBindingVolume;
import py.icshare.iscsiAccessRule.IscsiAccessRuleInformation;
import py.icshare.iscsiAccessRule.IscsiRuleRelationshipInformation;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.store.IscsiAccessRuleStore;
import py.infocenter.store.IscsiRuleRelationshipStore;
import py.infocenter.store.VolumeStore;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.*;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.when;

/**
 * A class includes some tests for canceling volume access rules.
 *
 */
public class CancelIscsiAccessRulesTest extends TestBase {

    private static final Logger logger = LoggerFactory.getLogger(CancelIscsiAccessRulesTest.class);

    private InformationCenterImpl icImpl;

    @Mock
    private IscsiAccessRuleStore iscsiAccessRuleStore;

    @Mock
    private IscsiRuleRelationshipStore iscsiRuleRelationshipStore;

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
        when(volumeStore.getVolume(anyLong())).thenReturn(volume);
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);

        icImpl.setIscsiAccessRuleStore(iscsiAccessRuleStore);
        icImpl.setIscsiRuleRelationshipStore(iscsiRuleRelationshipStore);
        icImpl.setVolumeStore(volumeStore);
        icImpl.setAppContext(appContext);
    }

    /**
     * A test for canceling iscsi access rules with false for commit field in request.
     *
     * @throws Exception
     */
    @Test
    public void testCancelIscsiAccessRulesWithoutCommit() throws Exception {
        CancelIscsiAccessRulesRequest request = new CancelIscsiAccessRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setDriverKey(new DriverKey_Thrift(0l,0l,0,DriverType_Thrift.ISCSI));
        request.setCommit(false);
        request.addToRuleIds(0l);
        request.addToRuleIds(1l);
        request.addToRuleIds(2l);
        request.addToRuleIds(3l);
        request.addToRuleIds(4l);

        // no affect for action "cancel" on deleting iscsi access rules
        when(iscsiAccessRuleStore.get(eq(0l))).thenReturn(buildIscsiAccessRuleInformation(0l, AccessRuleStatus.DELETING));
        when(iscsiAccessRuleStore.get(eq(1l))).thenReturn(buildIscsiAccessRuleInformation(1l, AccessRuleStatus.AVAILABLE));
        when(iscsiAccessRuleStore.get(eq(2l))).thenReturn(buildIscsiAccessRuleInformation(2l, AccessRuleStatus.AVAILABLE));
        when(iscsiAccessRuleStore.get(eq(3l))).thenReturn(buildIscsiAccessRuleInformation(3l, AccessRuleStatus.AVAILABLE));
        when(iscsiAccessRuleStore.get(eq(4l))).thenReturn(buildIscsiAccessRuleInformation(4l, AccessRuleStatus.AVAILABLE));

        when(iscsiRuleRelationshipStore.getByRuleId(eq(0l))).thenReturn(
                buildIscsiRelationshipInfoList(0l, 0l,0l,0,"ISCSI", AccessRuleStatusBindingVolume.FREE));
        when(iscsiRuleRelationshipStore.getByRuleId(eq(1l))).thenReturn(
                buildIscsiRelationshipInfoList(1l, 0l,0l,0,"ISCSI", AccessRuleStatusBindingVolume.APPLIED));
        when(iscsiRuleRelationshipStore.getByRuleId(eq(2l))).thenReturn(
                buildIscsiRelationshipInfoList(2l, 0l,0l,0,"ISCSI",AccessRuleStatusBindingVolume.APPLING));
        when(iscsiRuleRelationshipStore.getByRuleId(eq(3l))).thenReturn(
                buildIscsiRelationshipInfoList(3l, 0l,0l,0,"ISCSI", AccessRuleStatusBindingVolume.CANCELING));
        when(iscsiRuleRelationshipStore.getByRuleId(eq(4l))).thenReturn(
                buildIscsiRelationshipInfoList(4l, 0l,0l,0,"ISCSI",AccessRuleStatusBindingVolume.FREE));

        logger.warn("iscsiRuleRelationshipStore1 {}", iscsiRuleRelationshipStore.list().size());
        CancelIscsiAccessRulesResponse response = icImpl.cancelIscsiAccessRules(request);

        class IsCanceling extends ArgumentMatcher<IscsiRuleRelationshipInformation> {
            public boolean matches(Object o) {
                if (o == null) {
                    return false;
                }
                IscsiRuleRelationshipInformation relationshipInfo = (IscsiRuleRelationshipInformation) o;
                // turn "applied" volume access rules into "canceling"
                if (relationshipInfo.getRuleId() != 1l
                        || relationshipInfo.getStatus().equals(AccessRuleStatusBindingVolume.CANCELING.name())) {
                    return true;
                } else
                    return false;
            }
        }
        logger.warn("iscsiRuleRelationshipStore {}", iscsiRuleRelationshipStore.list());
        Mockito.verify(iscsiRuleRelationshipStore, Mockito.times(1)).save(argThat(new IsCanceling()));
        Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
        for (IscsiAccessRule_Thrift ruleFromRemote : response.getAirAccessRuleList()) {
            long ruleId = ruleFromRemote.getRuleId();
            Assert.assertTrue(ruleId == 0l || ruleId == 2l);
        }
    }

    /**
     * A test for canceling volume access rules with false for commit field in request.
     *
     * @throws Exception
     */
    @Test
    public void testCancelIscsiAccessRulesWithCommit() throws Exception {
        CancelIscsiAccessRulesRequest request = new CancelIscsiAccessRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setDriverKey(new DriverKey_Thrift(0l,0l,0,DriverType_Thrift.ISCSI));
        request.setCommit(true);
        request.addToRuleIds(0l);
        request.addToRuleIds(1l);
        request.addToRuleIds(2l);
        request.addToRuleIds(3l);
        request.addToRuleIds(4l);

        // no affect for action "cancel" on deleting volume access rules
        when(iscsiAccessRuleStore.get(eq(0l))).thenReturn(buildIscsiAccessRuleInformation(0l, AccessRuleStatus.DELETING));
        when(iscsiAccessRuleStore.get(eq(1l))).thenReturn(buildIscsiAccessRuleInformation(1l, AccessRuleStatus.AVAILABLE));
        when(iscsiAccessRuleStore.get(eq(2l))).thenReturn(buildIscsiAccessRuleInformation(2l, AccessRuleStatus.AVAILABLE));
        when(iscsiAccessRuleStore.get(eq(3l))).thenReturn(buildIscsiAccessRuleInformation(3l, AccessRuleStatus.AVAILABLE));
        when(iscsiAccessRuleStore.get(eq(4l))).thenReturn(buildIscsiAccessRuleInformation(4l, AccessRuleStatus.AVAILABLE));

        when(iscsiRuleRelationshipStore.getByRuleId(eq(0l))).thenReturn(
                buildIscsiRelationshipInfoList(0l, 0l,0l,0,"ISCSI",  AccessRuleStatusBindingVolume.FREE));
        when(iscsiRuleRelationshipStore.getByRuleId(eq(1l))).thenReturn(
                buildIscsiRelationshipInfoList(1l, 0l,0l,0,"ISCSI",  AccessRuleStatusBindingVolume.APPLIED));
        when(iscsiRuleRelationshipStore.getByRuleId(eq(2l))).thenReturn(
                buildIscsiRelationshipInfoList(2l, 0l,0l,0,"ISCSI", AccessRuleStatusBindingVolume.APPLING));
        when(iscsiRuleRelationshipStore.getByRuleId(eq(3l))).thenReturn(
                buildIscsiRelationshipInfoList(3l, 0l,0l,0,"ISCSI",  AccessRuleStatusBindingVolume.CANCELING));
        when(iscsiRuleRelationshipStore.getByRuleId(eq(4l))).thenReturn(
                buildIscsiRelationshipInfoList(4l, 0l,0l,0,"ISCSI", AccessRuleStatusBindingVolume.FREE));
        CancelIscsiAccessRulesResponse response = icImpl.cancelIscsiAccessRules(request);

        class IsFree extends ArgumentMatcher<Long> {
            public boolean matches(Object o) {
                if (o == null) {
                    return false;
                }
                long ruleId = (long) o;
                // delete "applied" and "canceling" volume access rules from relationship table
                if (ruleId == 1l ||ruleId == 3l) {
                    return true;
                } else
                    return false;
            }
        }

        Mockito.verify(iscsiRuleRelationshipStore, Mockito.times(2)).deleteByRuleIdandDriverKey(
                Matchers.any(), longThat(new IsFree()));
        Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
        for (IscsiAccessRule_Thrift ruleFromRemote : response.getAirAccessRuleList()) {
            long ruleId = ruleFromRemote.getRuleId();
            Assert.assertTrue(ruleId == 0l || ruleId == 2l);
        }
    }

    @Test
    public void testCancelIscsiAccessRuleAllApplied() throws Exception {
        CancelIscsiAccessRuleAllAppliedRequest request = new CancelIscsiAccessRuleAllAppliedRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setRuleId(0l);
        request.setCommit(true);
        request.addToDriverKeys(new DriverKey_Thrift(0l,0l,0,DriverType_Thrift.ISCSI));
        request.addToDriverKeys(new DriverKey_Thrift(0l,1l,0,DriverType_Thrift.ISCSI));
        request.addToDriverKeys(new DriverKey_Thrift(0l,2l,0,DriverType_Thrift.ISCSI));
        request.addToDriverKeys(new DriverKey_Thrift(0l,3l,0,DriverType_Thrift.ISCSI));
        request.addToDriverKeys(new DriverKey_Thrift(0l,4l,0,DriverType_Thrift.ISCSI));

        when(iscsiAccessRuleStore.get(eq(0l))).thenReturn(buildIscsiAccessRuleInformation(0l, AccessRuleStatus.AVAILABLE));

        List<IscsiRuleRelationshipInformation> relationList = new ArrayList<>();
        relationList.add(buildIscsiRelationshipInfoList(0l, 0l, 0l, 0, "ISCSI", AccessRuleStatusBindingVolume.FREE).get(0));
        relationList.add(buildIscsiRelationshipInfoList(0l, 0l, 1l, 0, "ISCSI", AccessRuleStatusBindingVolume.FREE).get(0));
        relationList.add(buildIscsiRelationshipInfoList(0l, 0l, 2l, 0, "ISCSI", AccessRuleStatusBindingVolume.APPLIED).get(0));
        relationList.add(buildIscsiRelationshipInfoList(0l, 0l, 3l, 0, "ISCSI", AccessRuleStatusBindingVolume.FREE).get(0));
        relationList.add(buildIscsiRelationshipInfoList(0l, 0l, 4l, 0, "ISCSI", AccessRuleStatusBindingVolume.APPLING).get(0));
        when(iscsiRuleRelationshipStore.getByRuleId(eq(0l))).thenReturn(relationList);

        CancelIscsiAccessRuleAllAppliedResponse response = icImpl.cancelIscsiAccessRuleAllApplied(request);

        class IsCanceling extends ArgumentMatcher<IscsiRuleRelationshipInformation> {
            public boolean matches(Object o) {
                if (o == null) {
                    return false;
                }
                IscsiRuleRelationshipInformation relationshipInfo = (IscsiRuleRelationshipInformation) o;
                // turn "applied" volume access rules into "canceling"
                if (relationshipInfo.getStatus().equals(AccessRuleStatusBindingVolume.APPLIED.name())) {
                    return true;
                } else
                    return false;
            }
        }
        logger.warn("iscsiRuleRelationshipStore {}", iscsiRuleRelationshipStore.list());

        //delete applied
        Mockito.verify(iscsiRuleRelationshipStore, Mockito.times(1)).deleteByRuleIdandDriverKey(
                new DriverKey_Thrift(0l,2l,0,DriverType_Thrift.ISCSI), 0l);
        // applying add to air list
        Assert.assertTrue(response.getAirDriverKeyListSize() == 1);
    }


    public IscsiAccessRuleInformation buildIscsiAccessRuleInformation(long ruleId, AccessRuleStatus status) {
        IscsiAccessRuleInformation iscsiAccessRuleInformation = new IscsiAccessRuleInformation();
        iscsiAccessRuleInformation.setRuleNotes("rule1");
        iscsiAccessRuleInformation.setInitiatorName("");
        iscsiAccessRuleInformation.setUser("");
        iscsiAccessRuleInformation.setPassed("");
        iscsiAccessRuleInformation.setPermission(2);
        iscsiAccessRuleInformation.setStatus(status.name());
        iscsiAccessRuleInformation.setRuleId(ruleId);
        return iscsiAccessRuleInformation;
    }

    public List<IscsiRuleRelationshipInformation> buildIscsiRelationshipInfoList(long ruleId, long did, long vid, int sid, String type,
                                                                                 AccessRuleStatusBindingVolume status) {
        IscsiRuleRelationshipInformation relationshipInfo = new IscsiRuleRelationshipInformation();
        relationshipInfo.setRelationshipId(RequestIdBuilder.get());
        relationshipInfo.setRuleId(ruleId);
        relationshipInfo.setDriverContainerId(did);
        relationshipInfo.setVolumeId(vid);
        relationshipInfo.setSnapshotId(sid);
        relationshipInfo.setDriverType(type);
        relationshipInfo.setStatus(status.name());
        List<IscsiRuleRelationshipInformation> list = new ArrayList<IscsiRuleRelationshipInformation>();
        list.add(relationshipInfo);
        return list;
    }
}
