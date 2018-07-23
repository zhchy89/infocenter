package py.infocenter.qos;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.icshare.StoragePoolStore;
import py.icshare.qos.MigrationRuleInformation;
import py.icshare.qos.MigrationRuleStore;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.service.InformationCenterImpl;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.*;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;


/**
 * A class includes some tests for creating  rules.
 */
public class CreateMigrateRuleTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(CreateMigrateRuleTest.class);

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
     * A test for create MigrationRule. In the case, new rules should save to db.
     *
     * @throws InvalidInputException_Thrift
     * @throws TException
     */
    @Test
    public void testCreateMigrateSpeed() throws InvalidInputException_Thrift,
            TException {
        MigrationRule_Thrift migrationSpeedFromRemote = new MigrationRule_Thrift();
        migrationSpeedFromRemote.setRuleId(100);
        migrationSpeedFromRemote.setMigrationRuleName("rule1");
        migrationSpeedFromRemote.setMaxMigrationSpeed(50);
        migrationSpeedFromRemote.setMigrationStrategy(MigrationStrategy_Thrift.Smart);
        migrationSpeedFromRemote.setMode(CheckSecondaryInactiveThresholdMode_Thrift.RelativeTime);
        migrationSpeedFromRemote.setStartTime(0);
        migrationSpeedFromRemote.setEndTime(0);
        migrationSpeedFromRemote.setWaitTime(0);
        migrationSpeedFromRemote.setWaitTime(0);
        migrationSpeedFromRemote.setIgnoreMissPagesAndLogs(false);

        CreateMigrationRulesRequest request = new CreateMigrationRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(100000);
        request.setMigrationRule(migrationSpeedFromRemote);

        icImpl.createMigrationRules(request);
        Mockito.verify(migrationRuleStore, Mockito.times(1)).save(any(MigrationRuleInformation.class));
    }
    /**
     * A test for create MigrationRule. In the case, new rules should save to db.
     *
     * @throws InvalidInputException_Thrift
     * @throws TException
     */
    @Test
    public void testListMigrateSpeed() throws InvalidInputException_Thrift,
            TException {
        ListMigrationRulesRequest request = new ListMigrationRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(100000);

        icImpl.listMigrationRules(request);
        Mockito.verify(migrationRuleStore, Mockito.times(1)).list();
    }

    /**
     * A test for create MigrationRule. same ruleId will cause a MigrationRuleDuplicate_Thrift exception
     *
     * @throws InvalidInputException_Thrift
     * @throws TException
     */
    @Test(expected = MigrationRuleDuplicate_Thrift.class)
    public void testCreateMigrateSpeed_2Times_SameRuleId() throws InvalidInputException_Thrift,
            TException {
        long ruleId = 100L;
        long maxMigrationSpeed = 50;
        MigrationRule_Thrift migrationSpeedFromRemote = new MigrationRule_Thrift();
        migrationSpeedFromRemote.setRuleId(ruleId);
        migrationSpeedFromRemote.setMigrationRuleName("rule1");
        migrationSpeedFromRemote.setMaxMigrationSpeed(maxMigrationSpeed);
        migrationSpeedFromRemote.setMigrationStrategy(MigrationStrategy_Thrift.Smart);
        migrationSpeedFromRemote.setMode(CheckSecondaryInactiveThresholdMode_Thrift.RelativeTime);
        migrationSpeedFromRemote.setStartTime(0);
        migrationSpeedFromRemote.setEndTime(0);
        migrationSpeedFromRemote.setWaitTime(0);
        migrationSpeedFromRemote.setWaitTime(0);
        migrationSpeedFromRemote.setIgnoreMissPagesAndLogs(false);

        CreateMigrationRulesRequest request = new CreateMigrationRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(100000);
        request.setMigrationRule(migrationSpeedFromRemote);

        List<MigrationRuleInformation> migrationSpeedInformationList = new ArrayList<>();
        MigrationRuleInformation migrationRuleInformation = new MigrationRuleInformation();
        migrationRuleInformation.setRuleId(ruleId);
        migrationRuleInformation.setMaxMigrationSpeed(maxMigrationSpeed+1);
        migrationSpeedInformationList.add(migrationRuleInformation);
        when(migrationRuleStore.list()).thenReturn(migrationSpeedInformationList);

        icImpl.createMigrationRules(request);
        Mockito.verify(migrationRuleStore, Mockito.times(1)).save(any(MigrationRuleInformation.class));
    }

    /**
     * A test for create MigrationRule. same maxMigrationSpeed but different ruleId will not cause a MigrationRuleDuplicate_Thrift exception
     *
     * @throws InvalidInputException_Thrift
     * @throws TException
     */
    @Test
    public void testCreateMigrateSpeed_2Times_SameSpeed_DiffRuleId() throws InvalidInputException_Thrift,
            TException {
        long ruleId = 100L;
        long maxMigrationSpeed = 50;
        MigrationRule_Thrift migrationSpeedFromRemote = new MigrationRule_Thrift();
        migrationSpeedFromRemote.setRuleId(ruleId);
        migrationSpeedFromRemote.setMigrationRuleName("rule1");
        migrationSpeedFromRemote.setMaxMigrationSpeed(maxMigrationSpeed);
        migrationSpeedFromRemote.setMigrationStrategy(MigrationStrategy_Thrift.Smart);
        migrationSpeedFromRemote.setMode(CheckSecondaryInactiveThresholdMode_Thrift.RelativeTime);
        migrationSpeedFromRemote.setStartTime(0);
        migrationSpeedFromRemote.setEndTime(0);
        migrationSpeedFromRemote.setWaitTime(0);
        migrationSpeedFromRemote.setWaitTime(0);
        migrationSpeedFromRemote.setIgnoreMissPagesAndLogs(false);

        CreateMigrationRulesRequest request = new CreateMigrationRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(100000);
        request.setMigrationRule(migrationSpeedFromRemote);

        List<MigrationRuleInformation> migrationSpeedInformationList = new ArrayList<>();
        MigrationRuleInformation migrationRuleInformation = new MigrationRuleInformation();
        migrationRuleInformation.setRuleId(ruleId+1);
        migrationRuleInformation.setMaxMigrationSpeed(maxMigrationSpeed);
        migrationSpeedInformationList.add(migrationRuleInformation);
        when(migrationRuleStore.list()).thenReturn(migrationSpeedInformationList);

        icImpl.createMigrationRules(request);
        Mockito.verify(migrationRuleStore, Mockito.times(1)).save(any(MigrationRuleInformation.class));
    }

}
