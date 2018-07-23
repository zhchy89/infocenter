package py.infocenter.service;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.icshare.iscsiAccessRule.IscsiAccessRuleInformation;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.store.IscsiAccessRuleStore;
import py.infocenter.store.IscsiRuleRelationshipStore;
import py.infocenter.store.VolumeStore;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.*;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;


/**
 * A class includes some tests for creating iscsi access rules.
 */
public class CreateIscsiAccessRulesTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(CreateIscsiAccessRulesTest.class);

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

        List<IscsiAccessRuleInformation> iscsiAccessRuleInformationList = new ArrayList<IscsiAccessRuleInformation>();
        when(iscsiAccessRuleStore.list()).thenReturn(iscsiAccessRuleInformationList);
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);

        icImpl.setIscsiAccessRuleStore(iscsiAccessRuleStore);
        icImpl.setIscsiRuleRelationshipStore(iscsiRuleRelationshipStore);
        icImpl.setVolumeStore(volumeStore);
        icImpl.setAppContext(appContext);
    }

    /**
     * A test for create iscsi access rules. In the case, new access rules should save to db.
     *
     * @throws IscsiAccessRuleDuplicate_Thrift
     * @throws InvalidInputException_Thrift
     * @throws TException
     */
    @Test
    public void testCreateIscsiAccessRules() throws IscsiAccessRuleDuplicate_Thrift, InvalidInputException_Thrift,
            TException {
        IscsiAccessRule_Thrift iscsiAccessRuleFromRemote = new IscsiAccessRule_Thrift();
        iscsiAccessRuleFromRemote.setRuleId(0l);
        iscsiAccessRuleFromRemote.setInitiatorName("iqn:");
        iscsiAccessRuleFromRemote.setUser("user1");
        iscsiAccessRuleFromRemote.setPassed("passwd1");
        iscsiAccessRuleFromRemote.setOutPassed("user2");
        iscsiAccessRuleFromRemote.setOutUser("passwd2");
        iscsiAccessRuleFromRemote.setPermission(AccessPermissionType_Thrift.READWRITE);
        CreateIscsiAccessRulesRequest request = new CreateIscsiAccessRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.addToAccessRules(iscsiAccessRuleFromRemote);

        icImpl.createIscsiAccessRules(request);
        Mockito.verify(iscsiAccessRuleStore, Mockito.times(1)).save(any(IscsiAccessRuleInformation.class));
    }



    /**
     * A test for create iscsi access rules. In the case, create error.
     * @throws ChapSameUserPasswdError_Thrift
     */
    @Test
    public void testCreateChapSameUserPasswd()throws IscsiAccessRuleDuplicate_Thrift, InvalidInputException_Thrift,
            TException {
        IscsiAccessRule_Thrift iscsiAccessRuleFromRemote = new IscsiAccessRule_Thrift();
        iscsiAccessRuleFromRemote.setRuleId(0l);
        iscsiAccessRuleFromRemote.setInitiatorName("iqn:");
        iscsiAccessRuleFromRemote.setUser("user1");
        iscsiAccessRuleFromRemote.setPassed("passwd1");
        iscsiAccessRuleFromRemote.setOutUser("user1");
        iscsiAccessRuleFromRemote.setOutPassed("passwd2");
        iscsiAccessRuleFromRemote.setPermission(AccessPermissionType_Thrift.READWRITE);
        CreateIscsiAccessRulesRequest request = new CreateIscsiAccessRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.addToAccessRules(iscsiAccessRuleFromRemote);

        boolean isException = false;
        try {
            icImpl.createIscsiAccessRules(request);
        } catch(ChapSameUserPasswdError_Thrift e){
            logger.error("create with the same user of incoming and outgoing user");
            isException = true;
        }
        assertTrue(isException);
    }

    /**
     * A test for create iscsi access rules. In the case, create error.
     * @throws ChapSameUserPasswdError_Thrift
     */
    @Test
    public void testCreateOnlyOutgoingUser()throws IscsiAccessRuleDuplicate_Thrift, InvalidInputException_Thrift,
            TException {
        IscsiAccessRule_Thrift iscsiAccessRuleFromRemote = new IscsiAccessRule_Thrift();
        iscsiAccessRuleFromRemote.setRuleId(0l);
        iscsiAccessRuleFromRemote.setInitiatorName("iqn:");
        iscsiAccessRuleFromRemote.setUser("");
        iscsiAccessRuleFromRemote.setPassed("");
        iscsiAccessRuleFromRemote.setOutUser("user1");
        iscsiAccessRuleFromRemote.setOutPassed("passwd2");
        iscsiAccessRuleFromRemote.setPermission(AccessPermissionType_Thrift.READWRITE);
        CreateIscsiAccessRulesRequest request = new CreateIscsiAccessRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.addToAccessRules(iscsiAccessRuleFromRemote);

        boolean isException = false;
        try {
            icImpl.createIscsiAccessRules(request);
        } catch(InvalidInputException_Thrift e){
            isException = true;
        }
        assertTrue(isException);
    }

}
