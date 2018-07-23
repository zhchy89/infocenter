package py.infocenter.service;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.longThat;
import static org.mockito.Matchers.any;

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
import py.thrift.share.DeleteVolumeAccessRulesRequest;
import py.thrift.share.DeleteVolumeAccessRulesResponse;
import py.thrift.share.VolumeAccessRule_Thrift;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;

/**
 * A class includes some test for delete volume access rules.
 * 
 * @author zjm
 *
 */
public class DeleteVolumeAccessRulesTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(DeleteVolumeAccessRulesTest.class);

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
     * A test for deleting volume access rules with false commit field. In the case, no volume access rules could be
     * delete.
     * 
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeAccessRulesWithoutCommit() throws Exception {
        DeleteVolumeAccessRulesRequest request = new DeleteVolumeAccessRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setCommit(false);
        request.addToRuleIds(0l);
        request.addToRuleIds(1l);
        request.addToRuleIds(2l);
        request.addToRuleIds(3l);
        request.addToRuleIds(4l);

        when(accessRuleStore.get(eq(0l))).thenReturn(buildAccessRuleInformation(0l, AccessRuleStatus.DELETING));
        when(accessRuleStore.get(eq(1l))).thenReturn(buildAccessRuleInformation(1l, AccessRuleStatus.AVAILABLE));
        when(accessRuleStore.get(eq(2l))).thenReturn(buildAccessRuleInformation(2l, AccessRuleStatus.AVAILABLE));
        when(accessRuleStore.get(eq(3l))).thenReturn(buildAccessRuleInformation(3l, AccessRuleStatus.AVAILABLE));
        when(accessRuleStore.get(eq(4l))).thenReturn(buildAccessRuleInformation(4l, AccessRuleStatus.AVAILABLE));

        when(volumeRuleRelationshipStore.getByRuleId(eq(0l))).thenReturn(
                buildRelationshipInfoList(0l, 0l, AccessRuleStatusBindingVolume.FREE));
        when(volumeRuleRelationshipStore.getByRuleId(eq(1l))).thenReturn(
                buildRelationshipInfoList(1l, 0l, AccessRuleStatusBindingVolume.APPLIED));
        // no affect on "appling" volume access rules after "delete" action
        when(volumeRuleRelationshipStore.getByRuleId(eq(2l))).thenReturn(
                buildRelationshipInfoList(2l, 0l, AccessRuleStatusBindingVolume.APPLING));
        // no affect on "canceling" volume access rules after "delete" action
        when(volumeRuleRelationshipStore.getByRuleId(eq(3l))).thenReturn(
                buildRelationshipInfoList(3l, 0l, AccessRuleStatusBindingVolume.CANCELING));
        when(volumeRuleRelationshipStore.getByRuleId(eq(4l))).thenReturn(
                buildRelationshipInfoList(4l, 0l, AccessRuleStatusBindingVolume.FREE));
        DeleteVolumeAccessRulesResponse response = icImpl.deleteVolumeAccessRules(request);

        Mockito.verify(accessRuleStore, Mockito.times(0)).delete(anyLong());
        Mockito.verify(accessRuleStore, Mockito.times(2)).save(any(AccessRuleInformation.class));
        Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
        for (VolumeAccessRule_Thrift ruleFromRemote : response.getAirAccessRuleList()) {
            long ruleId = ruleFromRemote.getRuleId();
            Assert.assertTrue(ruleId == 3l || ruleId == 2l);
        }
    }

    /**
     * A test for delete volume access rules with true commit field in request, In the case, all volume access rules
     * except appling and canceling rules could be delete.
     * 
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeAccessRulesWithConfirm() throws Exception {
        DeleteVolumeAccessRulesRequest request = new DeleteVolumeAccessRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setCommit(true);
        request.addToRuleIds(0l);
        request.addToRuleIds(1l);
        request.addToRuleIds(2l);
        request.addToRuleIds(3l);
        request.addToRuleIds(4l);

        when(accessRuleStore.get(eq(0l))).thenReturn(buildAccessRuleInformation(0l, AccessRuleStatus.DELETING));
        when(accessRuleStore.get(eq(1l))).thenReturn(buildAccessRuleInformation(1l, AccessRuleStatus.AVAILABLE));
        when(accessRuleStore.get(eq(2l))).thenReturn(buildAccessRuleInformation(2l, AccessRuleStatus.AVAILABLE));
        when(accessRuleStore.get(eq(3l))).thenReturn(buildAccessRuleInformation(3l, AccessRuleStatus.AVAILABLE));
        when(accessRuleStore.get(eq(4l))).thenReturn(buildAccessRuleInformation(4l, AccessRuleStatus.AVAILABLE));

        when(volumeRuleRelationshipStore.getByRuleId(eq(0l))).thenReturn(
                buildRelationshipInfoList(0l, 0l, AccessRuleStatusBindingVolume.FREE));
        when(volumeRuleRelationshipStore.getByRuleId(eq(1l))).thenReturn(
                buildRelationshipInfoList(1l, 0l, AccessRuleStatusBindingVolume.APPLIED));
        when(volumeRuleRelationshipStore.getByRuleId(eq(2l))).thenReturn(
                buildRelationshipInfoList(2l, 0l, AccessRuleStatusBindingVolume.APPLING));
        when(volumeRuleRelationshipStore.getByRuleId(eq(3l))).thenReturn(
                buildRelationshipInfoList(3l, 0l, AccessRuleStatusBindingVolume.CANCELING));
        when(volumeRuleRelationshipStore.getByRuleId(eq(4l))).thenReturn(
                buildRelationshipInfoList(4l, 0l, AccessRuleStatusBindingVolume.FREE));

        DeleteVolumeAccessRulesResponse response = icImpl.deleteVolumeAccessRules(request);

        class IsProperToDelete extends ArgumentMatcher<Long> {
            public boolean matches(Object o) {
                if (o == null) {
                    return false;
                }
                long ruleId = (long) o;
                // delete free, applied and deleting volume access rules
                if (ruleId == 0l || ruleId == 1l || ruleId == 4l) {
                    return true;
                } else
                    return false;
            }
        }
        ;

        Mockito.verify(accessRuleStore, Mockito.times(3)).delete(longThat(new IsProperToDelete()));
        Mockito.verify(volumeRuleRelationshipStore, Mockito.times(1)).deleteByRuleId(1l);
        Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
        for (VolumeAccessRule_Thrift ruleFromRemote : response.getAirAccessRuleList()) {
            long ruleId = ruleFromRemote.getRuleId();
            Assert.assertTrue(ruleId == 3l || ruleId == 2l);
        }
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
