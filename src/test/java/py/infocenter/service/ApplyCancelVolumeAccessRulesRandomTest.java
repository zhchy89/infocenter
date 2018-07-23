package py.infocenter.service;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
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
import py.thrift.share.ApplyVolumeAccessRulesRequest;
import py.thrift.share.ApplyVolumeAccessRulesResponse;
import py.thrift.share.CancelVolumeAccessRulesRequest;
import py.thrift.share.CancelVolumeAccessRulesResponse;
import py.thrift.share.VolumeBeingDeletedException_Thrift;
import py.thrift.share.VolumeNotFoundException_Thrift;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;

public class ApplyCancelVolumeAccessRulesRandomTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(ApplyVolumeAccessRuleTest.class);

    private InformationCenterImpl icImpl;
    
    private int aCount = 0;
    private int cCount = 0;

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
        when(volumeStore.getVolume(anyLong())).thenReturn(volume);
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);

        icImpl.setAccessRuleStore(accessRuleStore);
        icImpl.setVolumeRuleRelationshipStore(volumeRuleRelationshipStore);
        icImpl.setVolumeStore(volumeStore);
        icImpl.setAppContext(appContext);
    }

    @Test
    public void testApplyVolumeAccessRulesWithTwoVolumes() throws Exception {
        Configuration conf = new Configuration();
        testCase(conf);
        while(conf.next()) {
            testCase(conf);
        }
    }
    /**
     * 
     * @throws Exception
     */
    private void testCase(Configuration conf) throws Exception {
        System.out.println(conf.toString());
        ApplyVolumeAccessRulesRequest aRequest = new ApplyVolumeAccessRulesRequest();
        aRequest.setRequestId(RequestIdBuilder.get());
        aRequest.setVolumeId(0L);
        aRequest.setCommit(conf.applyCommit);
        aRequest.addToRuleIds(0L);
        
        CancelVolumeAccessRulesRequest cRequest = new CancelVolumeAccessRulesRequest();
        cRequest.setRequestId(RequestIdBuilder.get());
        cRequest.setVolumeId(1L);
        cRequest.setCommit(conf.cancelCommit);
        cRequest .addToRuleIds(0L);

        AccessRuleInformation ruleInfo = buildAccessRuleInformation(0L, AccessRuleStatus.AVAILABLE);
        
        when(accessRuleStore.get(eq(0L))).thenReturn(ruleInfo);

        VolumeRuleRelationshipInformation relation00 = new VolumeRuleRelationshipInformation(RequestIdBuilder.get(), 0l, 0l);
        relation00.setStatus(conf.applyStatus);

        VolumeRuleRelationshipInformation relation10 = new VolumeRuleRelationshipInformation(RequestIdBuilder.get(), 1l, 0l);
        relation10.setStatus(conf.cancelStatus);

        List<VolumeRuleRelationshipInformation> list = new ArrayList<VolumeRuleRelationshipInformation>();
        list.add(relation10);
        list.add(relation00);

        when(volumeRuleRelationshipStore.getByRuleId(eq(0L))).thenReturn(list);

        ApplyThread apply = new ApplyThread(aRequest);
        CancelThread cancel = new CancelThread(cRequest);
        
        Thread aThread = new Thread(apply);
        Thread cThread = new Thread(cancel);
        

        aThread.start();
        cThread.start();
        
        while (!apply.done || !cancel.done) {
            Thread.sleep(100);
        }

        ApplyVolumeAccessRulesResponse aResponse = apply.getResponse();
        CancelVolumeAccessRulesResponse cResponse = cancel.getResponse();
        
        
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

        Mockito.verify(volumeRuleRelationshipStore, Mockito.times(aCount)).save(argThat(new IsApplied()));
        Mockito.verify(volumeRuleRelationshipStore, Mockito.times(cCount)).deleteByRuleIdandVolumeID(1l, 0l);
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

    class ApplyThread implements Runnable {

        private ApplyVolumeAccessRulesRequest request;
        
        private ApplyVolumeAccessRulesResponse response;
        
        public boolean done = false;

        public ApplyThread(ApplyVolumeAccessRulesRequest request) {
            this.request = request;
        }

        public void run() {
            try {
                System.out.println("send request");
                response = icImpl.applyVolumeAccessRules(request);
                done = true;
            } catch (VolumeNotFoundException_Thrift e) {
                done = true;
                e.printStackTrace();
            } catch (VolumeBeingDeletedException_Thrift e) {
                done = true;
                e.printStackTrace();
            } catch (TException e) {
                done = true;
                e.printStackTrace();
            }
        }

        public ApplyVolumeAccessRulesResponse getResponse() {
            return response;
        }

        public void setResponse(ApplyVolumeAccessRulesResponse response) {
            this.response = response;
        }
    }
    
    class CancelThread implements Runnable {

        private CancelVolumeAccessRulesRequest request;
        
        private CancelVolumeAccessRulesResponse response;

        public boolean done = false;

        public CancelThread(CancelVolumeAccessRulesRequest request) {
            this.request = request;
        }

        public void run() {
            try {
                setResponse(icImpl.cancelVolumeAccessRules(request));
                done = true;
            } catch (VolumeNotFoundException_Thrift e) {
                done = true;
                e.printStackTrace();
            } catch (VolumeBeingDeletedException_Thrift e) {
                done = true;
                e.printStackTrace();
            } catch (TException e) {
                done = true;
                e.printStackTrace();
            }
        }

        public CancelVolumeAccessRulesResponse getResponse() {
            return response;
        }

        public void setResponse(CancelVolumeAccessRulesResponse response) {
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
        
        public Configuration () {
            aCommit = 0;
            aStatus = 0;
            cCommit = 0;
            cStatus = 0;
            count = 0;
            trans();
        }
        public boolean next() {
            cStatus ++;
            return adjust();
        }
        public boolean adjust() {
            if(cStatus > 3) {
                cStatus = 0;
                cCommit++;
            }
            if(cCommit > 1) {
                cCommit = 0;
                aStatus++;
            }
            if(aStatus > 3) {
                aStatus = 0;
                aCommit++;
            }
            if(aCommit > 1) {
                return false;
            }
            trans();
            return true;
        }
        public void trans() {
            if(aCommit == 0) {
                applyCommit = true;
            }
            else
                applyCommit = false;
            if(aStatus == 0)
                applyStatus = AccessRuleStatusBindingVolume.FREE.name();
            else if(aStatus == 1)
                applyStatus = AccessRuleStatusBindingVolume.APPLING.name();
            else if(aStatus == 2)
                applyStatus = AccessRuleStatusBindingVolume.APPLIED.name();
            else if(aStatus == 3)
                applyStatus = AccessRuleStatusBindingVolume.CANCELING.name();
            if(cCommit == 0) {
                cancelCommit = true;
            }
            else
                cancelCommit = false;
            if(cStatus == 0)
                cancelStatus = AccessRuleStatusBindingVolume.FREE.name();
            else if(cStatus == 1)
                cancelStatus = AccessRuleStatusBindingVolume.APPLING.name();
            else if(cStatus == 2)
                cancelStatus = AccessRuleStatusBindingVolume.APPLIED.name();
            else if(cStatus == 3)
                cancelStatus = AccessRuleStatusBindingVolume.CANCELING.name();
            if(applyCommit && (aStatus == 0 || aStatus == 1))
                aCount++;
            if(cancelCommit && (cStatus == 2 || cStatus == 3))
                cCount++;
        }
        @Override
        public String toString() {
            return "Configuration [aCommit=" + aCommit + ", aStatus=" + aStatus + ", cCommit=" + cCommit + ", cStatus=" + cStatus + ", applyCommit=" + applyCommit + ", cancelCommit=" + cancelCommit
                    + ", applyStatus=" + applyStatus + ", cancelStatus=" + cancelStatus + "]";
        }
    } 
}
