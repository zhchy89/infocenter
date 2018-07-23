package py.infocenter.service;

import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
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
import py.infocenter.store.*;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.*;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import java.util.ArrayList;
import java.util.List;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.longThat;
import static org.mockito.Matchers.any;


/**
 * A class includes some test for delete iscsi access rules.
 *
 */
public class DeleteIscsiAccessRulesTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(DeleteIscsiAccessRulesTest.class);

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
     * A test for deleting iscsi access rules with false commit field. In the case, no iscsi access rules could be
     * delete.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteIscsiAccessRulesWithoutCommit() throws Exception {
        DeleteIscsiAccessRulesRequest request = new DeleteIscsiAccessRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(1111);
        request.setCommit(false);
        request.addToRuleIds(0l);
        request.addToRuleIds(1l);
        request.addToRuleIds(2l);
        request.addToRuleIds(3l);
        request.addToRuleIds(4l);

        when(iscsiAccessRuleStore.get(eq(0l))).thenReturn(buildIscsiAccessRuleInformation(0l, AccessRuleStatus.DELETING));
        when(iscsiAccessRuleStore.get(eq(1l))).thenReturn(buildIscsiAccessRuleInformation(1l, AccessRuleStatus.AVAILABLE));
        when(iscsiAccessRuleStore.get(eq(2l))).thenReturn(buildIscsiAccessRuleInformation(2l, AccessRuleStatus.AVAILABLE));
        when(iscsiAccessRuleStore.get(eq(3l))).thenReturn(buildIscsiAccessRuleInformation(3l, AccessRuleStatus.AVAILABLE));
        when(iscsiAccessRuleStore.get(eq(4l))).thenReturn(buildIscsiAccessRuleInformation(4l, AccessRuleStatus.AVAILABLE));

        when(iscsiRuleRelationshipStore.getByRuleId(eq(0l))).thenReturn(
                buildIscsiRelationshipInfoList(0l, 0l,0l,0,"ISCSI",AccessRuleStatusBindingVolume.FREE));
        when(iscsiRuleRelationshipStore.getByRuleId(eq(1l))).thenReturn(
                buildIscsiRelationshipInfoList(1l, 0l,0l,0,"ISCSI", AccessRuleStatusBindingVolume.APPLIED));
        // no affect on "appling" volume access rules after "delete" action
        when(iscsiRuleRelationshipStore.getByRuleId(eq(2l))).thenReturn(
                buildIscsiRelationshipInfoList(2l, 0l,0l,0,"ISCSI", AccessRuleStatusBindingVolume.APPLING));
        // no affect on "canceling" volume access rules after "delete" action
        when(iscsiRuleRelationshipStore.getByRuleId(eq(3l))).thenReturn(
                buildIscsiRelationshipInfoList(3l, 0l,0l,0,"ISCSI",AccessRuleStatusBindingVolume.CANCELING));
        when(iscsiRuleRelationshipStore.getByRuleId(eq(4l))).thenReturn(
                buildIscsiRelationshipInfoList(4l, 0l,0l,0,"ISCSI", AccessRuleStatusBindingVolume.FREE));
        DeleteIscsiAccessRulesResponse response = icImpl.deleteIscsiAccessRules(request);

        Mockito.verify(iscsiAccessRuleStore, Mockito.times(0)).delete(anyLong());
        Mockito.verify(iscsiAccessRuleStore, Mockito.times(2)).save(any(IscsiAccessRuleInformation.class));
        Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
        for (IscsiAccessRule_Thrift ruleFromRemote : response.getAirAccessRuleList()) {
            long ruleId = ruleFromRemote.getRuleId();
            Assert.assertTrue(ruleId == 3l || ruleId == 2l);
        }
    }

    /**
     * A test for delete iscsi access rules with true commit field in request, In the case, all iscsi access rules
     * except appling and canceling rules could be delete.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteVolumeAccessRulesWithConfirm() throws Exception {
        DeleteIscsiAccessRulesRequest request = new DeleteIscsiAccessRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(1111);
        request.setCommit(true);
        request.addToRuleIds(0l);
        request.addToRuleIds(1l);
        request.addToRuleIds(2l);
        request.addToRuleIds(3l);
        request.addToRuleIds(4l);

        when(iscsiAccessRuleStore.get(eq(0l))).thenReturn(buildIscsiAccessRuleInformation(0l, AccessRuleStatus.DELETING));
        when(iscsiAccessRuleStore.get(eq(1l))).thenReturn(buildIscsiAccessRuleInformation(1l, AccessRuleStatus.AVAILABLE));
        when(iscsiAccessRuleStore.get(eq(2l))).thenReturn(buildIscsiAccessRuleInformation(2l, AccessRuleStatus.AVAILABLE));
        when(iscsiAccessRuleStore.get(eq(3l))).thenReturn(buildIscsiAccessRuleInformation(3l, AccessRuleStatus.AVAILABLE));
        when(iscsiAccessRuleStore.get(eq(4l))).thenReturn(buildIscsiAccessRuleInformation(4l, AccessRuleStatus.AVAILABLE));

        when(iscsiRuleRelationshipStore.getByRuleId(eq(0l))).thenReturn(
                buildIscsiRelationshipInfoList(0l, 0l,0l,0,"ISCSI", AccessRuleStatusBindingVolume.FREE));
        when(iscsiRuleRelationshipStore.getByRuleId(eq(1l))).thenReturn(
                buildIscsiRelationshipInfoList(1l, 0l,0l,0,"ISCSI",  AccessRuleStatusBindingVolume.APPLIED));
        when(iscsiRuleRelationshipStore.getByRuleId(eq(2l))).thenReturn(
                buildIscsiRelationshipInfoList(2l, 0l,0l,0,"ISCSI", AccessRuleStatusBindingVolume.APPLING));
        when(iscsiRuleRelationshipStore.getByRuleId(eq(3l))).thenReturn(
                buildIscsiRelationshipInfoList(3l, 0l,0l,0,"ISCSI",  AccessRuleStatusBindingVolume.CANCELING));
        when(iscsiRuleRelationshipStore.getByRuleId(eq(4l))).thenReturn(
                buildIscsiRelationshipInfoList(4l, 0l,0l,0,"ISCSI",  AccessRuleStatusBindingVolume.FREE));

        DeleteIscsiAccessRulesResponse response = icImpl.deleteIscsiAccessRules(request);

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

        Mockito.verify(iscsiAccessRuleStore, Mockito.times(3)).delete(longThat(new IsProperToDelete()));
        Mockito.verify(iscsiRuleRelationshipStore, Mockito.times(1)).deleteByRuleId(1l);
        Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
        for (IscsiAccessRule_Thrift ruleFromRemote : response.getAirAccessRuleList()) {
            long ruleId = ruleFromRemote.getRuleId();
            Assert.assertTrue(ruleId == 3l || ruleId == 2l);
        }
    }

    public IscsiAccessRuleInformation buildIscsiAccessRuleInformation(long ruleId, AccessRuleStatus status) {
        IscsiAccessRuleInformation iscsiAccessRuleInformation = new IscsiAccessRuleInformation();
        iscsiAccessRuleInformation.setInitiatorName("");
        iscsiAccessRuleInformation.setUser("");
        iscsiAccessRuleInformation.setPassed("");
        iscsiAccessRuleInformation.setPermission(2);
        iscsiAccessRuleInformation.setStatus(status.name());
        iscsiAccessRuleInformation.setRuleId(ruleId);
        return iscsiAccessRuleInformation;
    }

    public List<IscsiRuleRelationshipInformation> buildIscsiRelationshipInfoList(long ruleId, long did, long vid,int sid,String type,
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
