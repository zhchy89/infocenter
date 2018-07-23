package py.infocenter.qos;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.common.client.RequestResponseHelper;
import py.icshare.StoragePoolStore;
import py.icshare.qos.MigrationRule;
import py.icshare.qos.MigrationRuleInformation;
import py.icshare.qos.MigrationRuleStatus;
import py.icshare.qos.MigrationRuleStore;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.service.InformationCenterImpl;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.*;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

public class UpdateMigrateSpeedTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(UpdateMigrateSpeedTest.class);

    private InformationCenterImpl icImpl;

    @Mock
    private MigrationRuleStore migrationRuleStore;

    @Mock
    private StoragePoolStore storagePoolStore;

    @Mock
    private InfoCenterAppContext appContext;

    @Before
    public void init() throws Exception {
        super.init();

        icImpl = new InformationCenterImpl();

        List<MigrationRuleInformation> migrationSpeedInformationList = new ArrayList<>();
        when(migrationRuleStore.list()).thenReturn(migrationSpeedInformationList);
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);

        icImpl.setMigrationRuleStore(migrationRuleStore);
        icImpl.setStoragePoolStore(storagePoolStore);
        icImpl.setAppContext(appContext);
    }

    /**
     * A test for update MigrationRule. In the case, new rules should save to db.
     *
     * @throws Exception
     */
    @Test
    public void testCreateMigrateSpeed() throws Exception {

        MigrationRuleInformation migrationRuleInfo = ApplyMigrateRuleTest
                .buildMigrateRuleInformation(1L, MigrationRuleStatus.AVAILABLE);
        MigrationRule migrationRule = migrationRuleInfo.toMigrationRule();
        MigrationRule_Thrift migrationSpeedFromRemote = RequestResponseHelper.buildMigrationRuleThriftFrom(migrationRule);
        UpdateMigrationRulesRequest request = new UpdateMigrationRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(100000);
        request.setMigrationRule(migrationSpeedFromRemote);

        when(migrationRuleStore.get(eq(1L))).thenReturn(migrationRuleInfo);

        icImpl.updateMigrationRules(request);

        Mockito.verify(migrationRuleStore, Mockito.times(1)).update(any(MigrationRuleInformation.class));
    }

    /**
     * A test for create MigrationRule. In the case, new rules should save to db.
     */
    @Test
    public void testListMigrateSpeed() throws InvalidInputException_Thrift, TException {
        ListMigrationRulesRequest request = new ListMigrationRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(100000);

        icImpl.listMigrationRules(request);
        Mockito.verify(migrationRuleStore, Mockito.times(1)).list();
    }

}

