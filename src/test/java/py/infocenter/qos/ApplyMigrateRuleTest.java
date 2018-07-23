package py.infocenter.qos;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.icshare.Domain;
import py.icshare.StoragePool;
import py.icshare.StoragePoolStore;
import py.icshare.StoragePoolStrategy;
import py.icshare.qos.*;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.service.InformationCenterImpl;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.*;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ApplyMigrateRuleTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(ApplyMigrateRuleTest.class);

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

        List<MigrationRuleInformation> migrationSpeedInformationList = new ArrayList<MigrationRuleInformation>();
        when(migrationRuleStore.list()).thenReturn(migrationSpeedInformationList);
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);

        icImpl.setMigrationRuleStore(migrationRuleStore);
        icImpl.setStoragePoolStore(storagePoolStore);
        icImpl.setAppContext(appContext);
    }

    /**
     * A test for Apply MigrationRule.In the case, new rules id should save to related storage db.
     *
     * @throws InvalidInputException_Thrift
     * @throws TException
     */
    @Test
    public void testApplyMigrateSpeed() throws Exception {
        List<Long> poolIds = new ArrayList<>();
        poolIds.add(1L);

        ApplyMigrationRulesRequest request = new ApplyMigrationRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(100000);
        request.setRuleId(1L);
        request.setStoragePoolIds(poolIds);
        request.setCommit(true);

        when(storagePoolStore.getStoragePool(eq(1L))).thenReturn(buildStoragePool(1L));

        List<StoragePool> pools = buildStoragePoolList();
        when(storagePoolStore.listAllStoragePools()).thenReturn(pools);
        when(migrationRuleStore.get(eq(1L))).thenReturn(buildMigrateRuleInformation(1L, MigrationRuleStatus.AVAILABLE));

        icImpl.applyMigrationRules(request);

        Mockito.verify(storagePoolStore, Mockito.times(1)).saveStoragePool(any(StoragePool.class));
    }


    @Test
    public void testGetAppliedMigrateSpeed() throws Exception {
        GetAppliedStoragePoolsRequest request = new GetAppliedStoragePoolsRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(100000);
        request.setRuleId(1L);

        when(storagePoolStore.getStoragePool(eq(1L))).thenReturn(buildStoragePool(1L));

        List<StoragePool> pools = buildStoragePoolList();
        when(storagePoolStore.listAllStoragePools()).thenReturn(pools);
        when(migrationRuleStore.get(eq(1L))).thenReturn(buildMigrateRuleInformation(1L, MigrationRuleStatus.AVAILABLE));

        GetAppliedStoragePoolsResponse response = icImpl.getAppliedStoragePools(request);

        // null list
        assertEquals(0, response.getStoragePoolList().size());
    }

    @Test
    public void testGetAppliedMigrateSpeedOK() throws Exception {
        GetAppliedStoragePoolsRequest request = new GetAppliedStoragePoolsRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(100000);
        request.setRuleId(2L);

        when(storagePoolStore.getStoragePool(eq(1L))).thenReturn(buildStoragePool(1L));

        List<StoragePool> pools = buildStoragePoolList();
        when(storagePoolStore.listAllStoragePools()).thenReturn(pools);
        when(migrationRuleStore.get(eq(2L))).thenReturn(buildMigrateRuleInformation(1L, MigrationRuleStatus.AVAILABLE));

        GetAppliedStoragePoolsResponse response = icImpl.getAppliedStoragePools(request);

        assertNotNull(response.getStoragePoolList());
        assertEquals(1, response.getStoragePoolList().size());
    }



    public static MigrationRuleInformation buildMigrateRuleInformation(long ruleId, MigrationRuleStatus status) {
        MigrationRuleInformation migrationSpeedInformation = new MigrationRuleInformation();
        migrationSpeedInformation.setRuleId(ruleId);
        migrationSpeedInformation.setMigrationRuleName("rule");
        migrationSpeedInformation.setStatus(status.toString());
        migrationSpeedInformation.setMigrationStrategy(MigrationStrategy.Smart.name());
        migrationSpeedInformation.setMaxMigrationSpeed(100);

        migrationSpeedInformation.setStartTime(0L);
        migrationSpeedInformation.setEndTime(0L);
        migrationSpeedInformation.setWaitTime(0L);
        migrationSpeedInformation.setCheckSecondaryInactiveThresholdMode(CheckSecondaryInactiveThresholdMode.RelativeTime.name());
        migrationSpeedInformation.setIgnoreMissPagesAndLogs(false);
        return migrationSpeedInformation;
    }


    private List<StoragePool> buildStoragePoolList() {
        List<StoragePool> pools = new ArrayList<>();
        Long domainId = RequestIdBuilder.get();
        Long storagePoolId1 = RequestIdBuilder.get();
        Long storagePoolId2 = RequestIdBuilder.get();
        String storagePoolName1 = "STORAGEPOOLNAME1";
        String storagePoolName2 = "STORAGEPOOLNAME2";
        Domain domain = mock(Domain.class);
        when(domain.getDomainId()).thenReturn(domainId);
        StoragePool storagePool1 = new StoragePool();
        storagePool1.setDomainId(domainId);
        storagePool1.setPoolId(storagePoolId1);
        storagePool1.setName(storagePoolName1);
        storagePool1.setMigrationRuleId(3L);
        storagePool1.setStrategy(StoragePoolStrategy.Performance);

        StoragePool storagePool2 = new StoragePool();
        storagePool2.setDomainId(domainId);
        storagePool2.setPoolId(storagePoolId2);
        storagePool2.setName(storagePoolName2);
        storagePool2.setMigrationRuleId(2L);
        storagePool2.setStrategy(StoragePoolStrategy.Performance);

        pools.add(storagePool1);
        pools.add(storagePool2);

        return pools;
    }

    private StoragePool buildStoragePool(long ruleId) {
        Long domainId = RequestIdBuilder.get();
        String storagePoolName1 = "STORAGEPOOLNAME1";
        StoragePool storagePool = new StoragePool();
        storagePool.setDomainId(domainId);
        storagePool.setPoolId(ruleId);
        storagePool.setName(storagePoolName1);
        storagePool.setMigrationRuleId(1L);
        storagePool.setStrategy(StoragePoolStrategy.Performance);
        return storagePool;
    }


}

