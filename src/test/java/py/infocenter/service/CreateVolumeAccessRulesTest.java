package py.infocenter.service;

import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import py.app.context.AppContext;
import py.common.RequestIdBuilder;
import py.icshare.AccessRuleInformation;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.store.AccessRuleStore;
import py.infocenter.store.VolumeRuleRelationshipStore;
import py.infocenter.store.VolumeStore;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.AccessPermissionType_Thrift;
import py.thrift.share.CreateVolumeAccessRulesRequest;
import py.thrift.share.InvalidInputException_Thrift;
import py.thrift.share.VolumeAccessRuleDuplicate_Thrift;
import py.thrift.share.VolumeAccessRule_Thrift;

/**
 * A class includes some tests for creating volume access rules.
 * 
 * @author zjm
 *
 */
public class CreateVolumeAccessRulesTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(CreateVolumeAccessRulesTest.class);

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

        List<AccessRuleInformation> accessRuleInformationList = new ArrayList<AccessRuleInformation>();
        when(accessRuleStore.list()).thenReturn(accessRuleInformationList);
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);

        icImpl.setAccessRuleStore(accessRuleStore);
        icImpl.setVolumeRuleRelationshipStore(volumeRuleRelationshipStore);
        icImpl.setVolumeStore(volumeStore);
        icImpl.setAppContext(appContext);
    }

    /**
     * A test for create volume access rules. In the case, new access rules should save to db.
     * 
     * @throws VolumeAccessRuleDuplicate_Thrift
     * @throws InvalidInputException_Thrift
     * @throws TException
     */
    @Test
    public void testCreateVolumeAccessRules() throws VolumeAccessRuleDuplicate_Thrift, InvalidInputException_Thrift,
            TException {
        VolumeAccessRule_Thrift accessRuleFromRemote = new VolumeAccessRule_Thrift();
        accessRuleFromRemote.setRuleId(0l);
        accessRuleFromRemote.setIncomingHostName("10.0.1.16");
        accessRuleFromRemote.setPermission(AccessPermissionType_Thrift.READWRITE);
        CreateVolumeAccessRulesRequest request = new CreateVolumeAccessRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.addToAccessRules(accessRuleFromRemote);

        icImpl.createVolumeAccessRules(request);
        Mockito.verify(accessRuleStore, Mockito.times(1)).save(any(AccessRuleInformation.class));
    }
}
