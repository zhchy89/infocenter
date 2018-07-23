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
import py.driver.DriverMetadata;
import py.driver.DriverType;
import py.icshare.AccessRuleStatus;
import py.icshare.AccessRuleStatusBindingVolume;
import py.icshare.iscsiAccessRule.IscsiAccessRuleInformation;
import py.icshare.iscsiAccessRule.IscsiRuleRelationshipInformation;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.store.DriverStore;
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
 * A class includes some tests for applying iscsi access rule.
 */
public class ApplyIscsiAccessRuleTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(ApplyIscsiAccessRuleTest.class);

    private InformationCenterImpl icImpl;

    @Mock
    private IscsiAccessRuleStore iscsiAccessRuleStore;

    @Mock
    private IscsiRuleRelationshipStore iscsiRuleRelationshipStore;

    @Mock
    private VolumeStore volumeStore;

    @Mock
    private DriverStore driverStore;

    @Mock
    private InfoCenterAppContext appContext;

    @Before
    public void init() throws Exception {
        super.init();

        icImpl = new InformationCenterImpl();

        VolumeMetadata volume = new VolumeMetadata();
        volume.setVolumeStatus(VolumeStatus.Available);
        volume.setReadWrite(VolumeMetadata.ReadWriteType.READ_WRITE);
        when(volumeStore.getVolume(anyLong())).thenReturn(volume);
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);


        DriverMetadata driver1 = new DriverMetadata();
        driver1.setDriverContainerId(0l);
        driver1.setSnapshotId(0);
        driver1.setDriverType(DriverType.ISCSI);
        driver1.setVolumeId(0l);

        DriverMetadata driver2 = new DriverMetadata();
        driver2.setDriverContainerId(0l);
        driver2.setSnapshotId(0);
        driver2.setDriverType(DriverType.ISCSI);
        driver2.setVolumeId(1l);

        when(driverStore.get(0l, 0l, DriverType.ISCSI, 0)).thenReturn(driver1);
        when(driverStore.get(0l, 1l, DriverType.ISCSI, 0)).thenReturn(driver2);


        icImpl.setIscsiAccessRuleStore(iscsiAccessRuleStore);
        icImpl.setIscsiRuleRelationshipStore(iscsiRuleRelationshipStore);
        icImpl.setVolumeStore(volumeStore);
        icImpl.setDriverStore(driverStore);
        icImpl.setAppContext(appContext);
    }

    /**
     * Test for applying volume with false value for "commit" field in request. In the case, no access rules could be
     * applied to volume but status transfers to applying.
     *
     * @throws Exception
     */
    @Test
    public void testApplyIscsiAccessRulesWithoutCommit() throws Exception {
        ApplyIscsiAccessRulesRequest request = new ApplyIscsiAccessRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setDriverKey(new DriverKey_Thrift(0l, 0l, 0, DriverType_Thrift.ISCSI));
        request.setCommit(false);
        request.addToRuleIds(0l);
        request.addToRuleIds(1l);
        request.addToRuleIds(2l);
        request.addToRuleIds(3l);
        request.addToRuleIds(4l);

        // the access rule with current status deleting after action "apply" is still deleting
        when(iscsiAccessRuleStore.get(eq(0l))).thenReturn(buildIscsiAccessRuleInformation(0l, AccessRuleStatus.DELETING));
        when(iscsiAccessRuleStore.get(eq(1l))).thenReturn(buildIscsiAccessRuleInformation(1l, AccessRuleStatus.AVAILABLE));
        when(iscsiAccessRuleStore.get(eq(2l))).thenReturn(buildIscsiAccessRuleInformation(2l, AccessRuleStatus.AVAILABLE));
        when(iscsiAccessRuleStore.get(eq(3l))).thenReturn(buildIscsiAccessRuleInformation(3l, AccessRuleStatus.AVAILABLE));
        when(iscsiAccessRuleStore.get(eq(4l))).thenReturn(buildIscsiAccessRuleInformation(4l, AccessRuleStatus.AVAILABLE));

        when(iscsiRuleRelationshipStore.getByRuleId(eq(0l))).thenReturn(
                buildIscsiRelationshipInfoList(0l, 0l, 0l, 0, "ISCSI", AccessRuleStatusBindingVolume.FREE));
        when(iscsiRuleRelationshipStore.getByRuleId(eq(1l))).thenReturn(
                buildIscsiRelationshipInfoList(1l, 0l, 0l, 0, "ISCSI", AccessRuleStatusBindingVolume.APPLIED));
        when(iscsiRuleRelationshipStore.getByRuleId(eq(2l))).thenReturn(
                buildIscsiRelationshipInfoList(2l, 0l, 0l, 0, "ISCSI", AccessRuleStatusBindingVolume.APPLING));
        // the access rule with current status canceling after action "apply" is still canceling
        when(iscsiRuleRelationshipStore.getByRuleId(eq(3l))).thenReturn(
                buildIscsiRelationshipInfoList(3l, 0l, 0l, 0, "ISCSI", AccessRuleStatusBindingVolume.CANCELING));
        when(iscsiRuleRelationshipStore.getByRuleId(eq(4l))).thenReturn(
                buildIscsiRelationshipInfoList(4l, 0l, 0l, 0, "ISCSI", AccessRuleStatusBindingVolume.FREE));

        ApplyIscsiAccessRulesResponse response = icImpl.applyIscsiAccessRules(request);

        class IsAppling extends ArgumentMatcher<IscsiRuleRelationshipInformation> {
            public boolean matches(Object o) {
                if (o == null) {
                    return false;
                }
                IscsiRuleRelationshipInformation relationshipInfo = (IscsiRuleRelationshipInformation) o;
                // the status of free access rules could be applying after action "apply"
                if (relationshipInfo.getRuleId() != 4l
                        || relationshipInfo.getStatus().equals(AccessRuleStatusBindingVolume.APPLING.name())) {
                    return true;
                } else
                    return false;
            }
        }
        ;

        Mockito.verify(iscsiRuleRelationshipStore, Mockito.times(1)).save(argThat(new IsAppling()));
        Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
        for (IscsiAccessRule_Thrift ruleFromRemote : response.getAirAccessRuleList()) {
            long ruleId = ruleFromRemote.getRuleId();
            Assert.assertTrue(ruleId == 0l || ruleId == 3l);
        }
    }

    /**
     * Test for applying volume with true value for "commit" field in request. In the case, all access rules except
     * those who are canceling or deleting could be applied to volume.
     *
     * @throws Exception
     */
    @Test
    public void testApplyIscsiAccessRulesWithCommit() throws Exception {
        ApplyIscsiAccessRulesRequest request = new ApplyIscsiAccessRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setDriverKey(new DriverKey_Thrift(0l, 0l, 0, DriverType_Thrift.ISCSI));
        request.setCommit(true);
        request.addToRuleIds(0l);
        request.addToRuleIds(1l);
        request.addToRuleIds(2l);
        request.addToRuleIds(3l);
        request.addToRuleIds(4l);

        // no affect after action "apply" on "deleting" volume access rules
        when(iscsiAccessRuleStore.get(eq(0l))).thenReturn(buildIscsiAccessRuleInformation(0l, AccessRuleStatus.DELETING));
        when(iscsiAccessRuleStore.get(eq(1l))).thenReturn(buildIscsiAccessRuleInformation(1l, AccessRuleStatus.AVAILABLE));
        when(iscsiAccessRuleStore.get(eq(2l))).thenReturn(buildIscsiAccessRuleInformation(2l, AccessRuleStatus.AVAILABLE));
        when(iscsiAccessRuleStore.get(eq(3l))).thenReturn(buildIscsiAccessRuleInformation(3l, AccessRuleStatus.AVAILABLE));
        when(iscsiAccessRuleStore.get(eq(4l))).thenReturn(buildIscsiAccessRuleInformation(4l, AccessRuleStatus.AVAILABLE));

        when(iscsiRuleRelationshipStore.getByRuleId(eq(0l))).thenReturn(
                buildIscsiRelationshipInfoList(0l, 0l, 0l, 0, "ISCSI", AccessRuleStatusBindingVolume.FREE));
        when(iscsiRuleRelationshipStore.getByRuleId(eq(1l))).thenReturn(
                buildIscsiRelationshipInfoList(1l, 0l, 0l, 0, "ISCSI", AccessRuleStatusBindingVolume.APPLIED));
        when(iscsiRuleRelationshipStore.getByRuleId(eq(2l))).thenReturn(
                buildIscsiRelationshipInfoList(2l, 0l, 0l, 0, "ISCSI", AccessRuleStatusBindingVolume.APPLING));
        // the access rule with current status canceling after action "apply" is still canceling
        when(iscsiRuleRelationshipStore.getByRuleId(eq(3l))).thenReturn(
                buildIscsiRelationshipInfoList(3l, 0l, 0l, 0, "ISCSI", AccessRuleStatusBindingVolume.CANCELING));
        when(iscsiRuleRelationshipStore.getByRuleId(eq(4l))).thenReturn(
                buildIscsiRelationshipInfoList(4l, 0l, 0l, 0, "ISCSI", AccessRuleStatusBindingVolume.FREE));

        ApplyIscsiAccessRulesResponse response = icImpl.applyIscsiAccessRules(request);

        class IsApplied extends ArgumentMatcher<IscsiRuleRelationshipInformation> {
            public boolean matches(Object o) {
                if (o == null) {
                    return false;
                }
                IscsiRuleRelationshipInformation relationshipInfo = (IscsiRuleRelationshipInformation) o;
                // "free" and "appling" volume access rules turn into "applied" after action "apply"
                if (relationshipInfo.getRuleId() != 4l || relationshipInfo.getRuleId() != 2l
                        || relationshipInfo.getStatus().equals(AccessRuleStatusBindingVolume.APPLIED.name())) {
                    return true;
                } else
                    return false;
            }
        }

        Mockito.verify(iscsiRuleRelationshipStore, Mockito.times(2)).save(argThat(new IsApplied()));
        Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
        for (IscsiAccessRule_Thrift ruleFromRemote : response.getAirAccessRuleList()) {
            long ruleId = ruleFromRemote.getRuleId();
            Assert.assertTrue(ruleId == 0l || ruleId == 3l);
        }
    }

    @Test
    public void testaApplyIscsiAccessRuleOnIscsis() throws Exception {
        ApplyIscsiAccessRuleOnIscsisRequest request = new ApplyIscsiAccessRuleOnIscsisRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setRuleId(0l);
        request.setCommit(true);
        request.addToDriverKeys(new DriverKey_Thrift(0l, 0l, 0, DriverType_Thrift.ISCSI));
        request.addToDriverKeys(new DriverKey_Thrift(0l, 1l, 0, DriverType_Thrift.ISCSI));
        request.addToDriverKeys(new DriverKey_Thrift(0l, 2l, 0, DriverType_Thrift.ISCSI));
        request.addToDriverKeys(new DriverKey_Thrift(0l, 3l, 0, DriverType_Thrift.ISCSI));
        request.addToDriverKeys(new DriverKey_Thrift(0l, 4l, 0, DriverType_Thrift.ISCSI));

        when(iscsiAccessRuleStore.get(eq(0l))).thenReturn(buildIscsiAccessRuleInformation(0l, AccessRuleStatus.AVAILABLE));

        List<IscsiRuleRelationshipInformation> relationList = new ArrayList<>();
        relationList.add(buildIscsiRelationshipInfoList(0l, 0l, 0l, 0, "ISCSI", AccessRuleStatusBindingVolume.FREE).get(0));
        relationList.add(buildIscsiRelationshipInfoList(0l, 0l, 1l, 0, "ISCSI", AccessRuleStatusBindingVolume.FREE).get(0));
        relationList.add(buildIscsiRelationshipInfoList(0l, 0l, 2l, 0, "ISCSI", AccessRuleStatusBindingVolume.FREE).get(0));
        relationList.add(buildIscsiRelationshipInfoList(0l, 0l, 3l, 0, "ISCSI", AccessRuleStatusBindingVolume.FREE).get(0));
        relationList.add(buildIscsiRelationshipInfoList(0l, 0l, 4l, 0, "ISCSI", AccessRuleStatusBindingVolume.FREE).get(0));

        when(iscsiRuleRelationshipStore.getByRuleId(eq(0l))).thenReturn(relationList);

        ApplyIscsiAccessRuleOnIscsisResponse response = icImpl.applyIscsiAccessRuleOnIscsis(request);

        // driverstore only have two drivers
        Assert.assertTrue(response.getAirDriverKeyListSize() == 3);
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
