package py.infocenter.service;
import org.apache.thrift.TException;
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
import py.infocenter.store.*;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.*;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import java.util.ArrayList;
import java.util.List;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.when;

public class ApplyCancelIscsiAccessRulesRandomTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(ApplyCancelIscsiAccessRulesRandomTest.class);

    private InformationCenterImpl icImpl;

    private int aCount = 0;
    private int cCount = 0;

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
        new DriverKey_Thrift(0l,0l,0,DriverType_Thrift.ISCSI);

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

        when(driverStore.get(0l,0l,DriverType.ISCSI,0)).thenReturn(driver1);
        when(driverStore.get(0l,1l,DriverType.ISCSI,0)).thenReturn(driver2);


        icImpl.setIscsiAccessRuleStore(iscsiAccessRuleStore);
        icImpl.setIscsiRuleRelationshipStore(iscsiRuleRelationshipStore);
        icImpl.setVolumeStore(volumeStore);
        icImpl.setAppContext(appContext);
        icImpl.setDriverStore(driverStore);
    }

    @Test
    public void testApplyIscsiAccessRulesWithTwoIscsis() throws Exception {
        Configuration conf = new Configuration();
        testCase(conf);
        while (conf.next()) {
            testCase(conf);
        }
    }

    private void testCase(Configuration conf) throws Exception {
        System.out.println(conf.toString());
        ApplyIscsiAccessRulesRequest aRequest = new ApplyIscsiAccessRulesRequest();
        aRequest.setRequestId(RequestIdBuilder.get());
        aRequest.setDriverKey(new DriverKey_Thrift(0l,0l,0,DriverType_Thrift.ISCSI));
        aRequest.setCommit(conf.applyCommit);
        aRequest.addToRuleIds(0l);

        CancelIscsiAccessRulesRequest cRequest = new CancelIscsiAccessRulesRequest();
        cRequest.setRequestId(RequestIdBuilder.get());
        cRequest.setDriverKey(new DriverKey_Thrift(0l,1l,0,DriverType_Thrift.ISCSI));
        cRequest.setCommit(conf.cancelCommit);
        cRequest.addToRuleIds(0l);

        IscsiAccessRuleInformation ruleInfo = buildIscsiAccessRuleInformation(0l, AccessRuleStatus.AVAILABLE);

        when(iscsiAccessRuleStore.get(eq(0l))).thenReturn(ruleInfo);

        IscsiRuleRelationshipInformation relation00 = new IscsiRuleRelationshipInformation(RequestIdBuilder.get(), 0l,0l,0,"ISCSI", 0l);
        relation00.setStatus(conf.applyStatus);

        IscsiRuleRelationshipInformation relation10 = new IscsiRuleRelationshipInformation(RequestIdBuilder.get(),0l,1l,0,"ISCSI", 0l);
        relation10.setStatus(conf.cancelStatus);

        List<IscsiRuleRelationshipInformation> list = new ArrayList<IscsiRuleRelationshipInformation>();
        list.add(relation10);
        list.add(relation00);

        when(iscsiRuleRelationshipStore.getByRuleId(eq(0l))).thenReturn(list);

        ApplyThread apply = new ApplyThread(aRequest);
        CancelThread cancel = new CancelThread(cRequest);

        Thread aThread = new Thread(apply);
        Thread cThread = new Thread(cancel);

        boolean applySwitch = false;
        applySwitch = true;
        boolean cancelSwitch = false;
        cancelSwitch = true;

        if (applySwitch)
            aThread.start();
        if (cancelSwitch)
            cThread.start();

        while ((applySwitch && !apply.done) || (cancelSwitch && !cancel.done)) {
            Thread.sleep(100);
        }

        ApplyIscsiAccessRulesResponse aResponse = apply.getResponse();
        CancelIscsiAccessRulesResponse cResponse = cancel.getResponse();


        class IsApplied extends ArgumentMatcher<IscsiRuleRelationshipInformation> {
            public boolean matches(Object o) {
                if (o == null) {
                    return false;
                }
                IscsiRuleRelationshipInformation relationshipInfo = (IscsiRuleRelationshipInformation) o;
                // "free" and "appling" volume access rules turn into "applied" after action "apply"
                if (relationshipInfo.getStatus().equals(AccessRuleStatusBindingVolume.APPLIED.name())) {
                    return true;
                } else
                    return false;
            }
        }

        if (applySwitch)
            Mockito.verify(iscsiRuleRelationshipStore, Mockito.times(aCount)).save(argThat(new IsApplied()));
        if (cancelSwitch)
            Mockito.verify(iscsiRuleRelationshipStore, Mockito.times(cCount)).deleteByRuleIdandDriverKey(new DriverKey_Thrift(0l,1l,0,DriverType_Thrift.ISCSI), 0l);
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

    class ApplyThread implements Runnable {

        private ApplyIscsiAccessRulesRequest request;

        private ApplyIscsiAccessRulesResponse response;

        public boolean done = false;

        public ApplyThread(ApplyIscsiAccessRulesRequest request) {
            this.request = request;
        }

        public void run() {
            try {
                System.out.println("send request");
                response = icImpl.applyIscsiAccessRules(request);
                done = true;
            } catch (IscsiNotFoundException_Thrift e) {
                done = true;
                e.printStackTrace();
            } catch (IscsiBeingDeletedException_Thrift e) {
                done = true;
                e.printStackTrace();
            } catch (TException e) {
                done = true;
                e.printStackTrace();
            }
        }

        public ApplyIscsiAccessRulesResponse getResponse() {
            return response;
        }

        public void setResponse(ApplyIscsiAccessRulesResponse response) {
            this.response = response;
        }
    }

    class CancelThread implements Runnable {

        private CancelIscsiAccessRulesRequest request;

        private CancelIscsiAccessRulesResponse response;

        public boolean done = false;

        public CancelThread(CancelIscsiAccessRulesRequest request) {
            this.request = request;
        }

        public void run() {
            try {
                setResponse(icImpl.cancelIscsiAccessRules(request));
                done = true;
            } catch (IscsiNotFoundException_Thrift e) {
                done = true;
                e.printStackTrace();
            } catch (IscsiBeingDeletedException_Thrift e) {
                done = true;
                e.printStackTrace();
            } catch (TException e) {
                done = true;
                e.printStackTrace();
            }
        }

        public CancelIscsiAccessRulesResponse getResponse() {
            return response;
        }

        public void setResponse(CancelIscsiAccessRulesResponse response) {
            this.response = response;
        }
    }

    class Configuration {
        public int aCommit;
        public int aStatus;
        public int cCommit;
        public int cStatus;

        public boolean applyCommit;
        public boolean cancelCommit;
        public String applyStatus;
        public String cancelStatus;

        public int count;

        public Configuration() {
            aCommit = 0;
            aStatus = 0;
            cCommit = 0;
            cStatus = 0;
            count = 0;
            trans();
        }

        public boolean next() {
            cStatus++;
            return adjust();
        }

        public boolean adjust() {
            if (cStatus > 3) {
                cStatus = 0;
                cCommit++;
            }
            if (cCommit > 1) {
                cCommit = 0;
                aStatus++;
            }
            if (aStatus > 3) {
                aStatus = 0;
                aCommit++;
            }
            if (aCommit > 1) {
                return false;
            }
            trans();
            return true;
        }

        public void trans() {
            if (aCommit == 0) {
                applyCommit = true;
            } else
                applyCommit = false;
            if (aStatus == 0)
                applyStatus = AccessRuleStatusBindingVolume.FREE.name();
            else if (aStatus == 1)
                applyStatus = AccessRuleStatusBindingVolume.APPLING.name();
            else if (aStatus == 2)
                applyStatus = AccessRuleStatusBindingVolume.APPLIED.name();
            else if (aStatus == 3)
                applyStatus = AccessRuleStatusBindingVolume.CANCELING.name();
            if (cCommit == 0) {
                cancelCommit = true;
            } else
                cancelCommit = false;
            if (cStatus == 0)
                cancelStatus = AccessRuleStatusBindingVolume.FREE.name();
            else if (cStatus == 1)
                cancelStatus = AccessRuleStatusBindingVolume.APPLING.name();
            else if (cStatus == 2)
                cancelStatus = AccessRuleStatusBindingVolume.APPLIED.name();
            else if (cStatus == 3)
                cancelStatus = AccessRuleStatusBindingVolume.CANCELING.name();
            if (applyCommit && (aStatus == 0 || aStatus == 1))
                aCount++;
            if (cancelCommit && (cStatus == 2 || cStatus == 3))
                cCount++;
        }

        @Override
        public String toString() {
            return "Configuration [aCommit=" + aCommit + ", aStatus=" + aStatus + ", cCommit=" + cCommit + ", cStatus=" + cStatus + ", applyCommit=" + applyCommit + ", cancelCommit=" + cancelCommit
                    + ", applyStatus=" + applyStatus + ", cancelStatus=" + cancelStatus + "]";
        }
    }
}
