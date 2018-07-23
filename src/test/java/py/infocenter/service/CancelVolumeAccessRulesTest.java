package py.infocenter.service;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.longThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import py.app.context.AppContext;
import py.common.RequestIdBuilder;
import py.icshare.AccessRuleInformation;
import py.icshare.AccessRuleStatus;
import py.icshare.AccessRuleStatusBindingVolume;
import py.icshare.VolumeRuleRelationshipInformation;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.store.AccessRuleStore;
import py.infocenter.store.VolumeRuleRelationshipStore;
import py.infocenter.store.VolumeStore;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.*;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;

/**
 * A class includes some tests for canceling volume access rules.
 * 
 * @author zjm
 *
 */
public class CancelVolumeAccessRulesTest extends TestBase {

    private static final Logger logger = LoggerFactory.getLogger(CancelVolumeAccessRulesTest.class);

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
        when(volumeStore.getVolume(anyLong())).thenReturn(volume);
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);

        icImpl.setAccessRuleStore(accessRuleStore);
        icImpl.setVolumeRuleRelationshipStore(volumeRuleRelationshipStore);
        icImpl.setVolumeStore(volumeStore);
        icImpl.setAppContext(appContext);
    }

    /**
     * A test for canceling volume access rules with false for commit field in request.
     * 
     * @throws Exception
     */
    @Test
    public void testCancelVolumeAccessRulesWithoutCommit() throws Exception {
        CancelVolumeAccessRulesRequest request = new CancelVolumeAccessRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(0l);
        request.setCommit(false);
        request.addToRuleIds(0l);
        request.addToRuleIds(1l);
        request.addToRuleIds(2l);
        request.addToRuleIds(3l);
        request.addToRuleIds(4l);

        // no affect for action "cancel" on deleting volume access rules
        when(accessRuleStore.get(eq(0l))).thenReturn(buildAccessRuleInformation(0l, AccessRuleStatus.DELETING));
        when(accessRuleStore.get(eq(1l))).thenReturn(buildAccessRuleInformation(1l, AccessRuleStatus.AVAILABLE));
        when(accessRuleStore.get(eq(2l))).thenReturn(buildAccessRuleInformation(2l, AccessRuleStatus.AVAILABLE));
        when(accessRuleStore.get(eq(3l))).thenReturn(buildAccessRuleInformation(3l, AccessRuleStatus.AVAILABLE));
        when(accessRuleStore.get(eq(4l))).thenReturn(buildAccessRuleInformation(4l, AccessRuleStatus.AVAILABLE));

        when(volumeRuleRelationshipStore.getByRuleId(eq(0l))).thenReturn(
                buildRelationshipInfoList(0l, 0l, AccessRuleStatusBindingVolume.FREE));
        when(volumeRuleRelationshipStore.getByRuleId(eq(1l))).thenReturn(
                buildRelationshipInfoList(1l, 0l, AccessRuleStatusBindingVolume.APPLIED));
        // no affect for action "cancel" on appling volume access rules
        when(volumeRuleRelationshipStore.getByRuleId(eq(2l))).thenReturn(
                buildRelationshipInfoList(2l, 0l, AccessRuleStatusBindingVolume.APPLING));
        when(volumeRuleRelationshipStore.getByRuleId(eq(3l))).thenReturn(
                buildRelationshipInfoList(3l, 0l, AccessRuleStatusBindingVolume.CANCELING));
        when(volumeRuleRelationshipStore.getByRuleId(eq(4l))).thenReturn(
                buildRelationshipInfoList(4l, 0l, AccessRuleStatusBindingVolume.FREE));
        CancelVolumeAccessRulesResponse response = icImpl.cancelVolumeAccessRules(request);

        class IsCanceling extends ArgumentMatcher<VolumeRuleRelationshipInformation> {
            public boolean matches(Object o) {
                if (o == null) {
                    return false;
                }
                VolumeRuleRelationshipInformation relationshipInfo = (VolumeRuleRelationshipInformation) o;
                // turn "applied" volume access rules into "canceling"
                if (relationshipInfo.getRuleId() != 1l
                        || relationshipInfo.getStatus().equals(AccessRuleStatusBindingVolume.CANCELING.name())) {
                    return true;
                } else
                    return false;
            }
        }

        Mockito.verify(volumeRuleRelationshipStore, Mockito.times(1)).save(argThat(new IsCanceling()));
        Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
        for (VolumeAccessRule_Thrift ruleFromRemote: response.getAirAccessRuleList()) {
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
    public void testCancelVolumeAccessRulesWithCommit() throws Exception {
        CancelVolumeAccessRulesRequest request = new CancelVolumeAccessRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(0l);
        request.setCommit(true);
        request.addToRuleIds(0l);
        request.addToRuleIds(1l);
        request.addToRuleIds(2l);
        request.addToRuleIds(3l);
        request.addToRuleIds(4l);

        // no affect for action "cancel" on deleting volume access rules
        when(accessRuleStore.get(eq(0l))).thenReturn(buildAccessRuleInformation(0l, AccessRuleStatus.DELETING));
        when(accessRuleStore.get(eq(1l))).thenReturn(buildAccessRuleInformation(1l, AccessRuleStatus.AVAILABLE));
        when(accessRuleStore.get(eq(2l))).thenReturn(buildAccessRuleInformation(2l, AccessRuleStatus.AVAILABLE));
        when(accessRuleStore.get(eq(3l))).thenReturn(buildAccessRuleInformation(3l, AccessRuleStatus.AVAILABLE));
        when(accessRuleStore.get(eq(4l))).thenReturn(buildAccessRuleInformation(4l, AccessRuleStatus.AVAILABLE));

        when(volumeRuleRelationshipStore.getByRuleId(eq(0l))).thenReturn(
                buildRelationshipInfoList(0l, 0l, AccessRuleStatusBindingVolume.FREE));
        when(volumeRuleRelationshipStore.getByRuleId(eq(1l))).thenReturn(
                buildRelationshipInfoList(1l, 0l, AccessRuleStatusBindingVolume.APPLIED));
        // no affect for action "appling" on deleting volume access rules
        when(volumeRuleRelationshipStore.getByRuleId(eq(2l))).thenReturn(
                buildRelationshipInfoList(2l, 0l, AccessRuleStatusBindingVolume.APPLING));
        when(volumeRuleRelationshipStore.getByRuleId(eq(3l))).thenReturn(
                buildRelationshipInfoList(3l, 0l, AccessRuleStatusBindingVolume.CANCELING));
        when(volumeRuleRelationshipStore.getByRuleId(eq(4l))).thenReturn(
                buildRelationshipInfoList(4l, 0l, AccessRuleStatusBindingVolume.FREE));
        CancelVolumeAccessRulesResponse response = icImpl.cancelVolumeAccessRules(request);

        class IsFree extends ArgumentMatcher<Long> {
            public boolean matches(Object o) {
                if (o == null) {
                    return false;
                }
                long ruleId = (long) o;
                // delete "applied" and "canceling" volume access rules from relationship table
                if (ruleId == 1l || ruleId == 3l) {
                    return true;
                } else
                    return false;
            }
        }

        Mockito.verify(volumeRuleRelationshipStore, Mockito.times(2)).deleteByRuleIdandVolumeID(eq(0l),
                longThat(new IsFree()));
        Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
        for (VolumeAccessRule_Thrift ruleFromRemote: response.getAirAccessRuleList()) {
            long ruleId = ruleFromRemote.getRuleId();
            Assert.assertTrue(ruleId == 0l || ruleId == 2l);
        }
    }


    @Test
    public void testCancelVolumeAccessRuleAllApplied() throws Exception {
        CancelVolAccessRuleAllAppliedRequest request = new CancelVolAccessRuleAllAppliedRequest();
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

        CancelVolAccessRuleAllAppliedResponse response = icImpl.cancelVolAccessRuleAllApplied(request);

        assert(response.getAirVolumeIdsSize() == 1);
    }


    public AccessRuleInformation buildAccessRuleInformation(long ruleId, AccessRuleStatus status) {
        AccessRuleInformation accessRuleInformation = new AccessRuleInformation();
        accessRuleInformation.setIpAddress("");
        accessRuleInformation.setPermission(2);
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

        List<VolumeRuleRelationshipInformation> list = new ArrayList<VolumeRuleRelationshipInformation>();
        list.add(relationshipInfo);
        return list;
    }
}
