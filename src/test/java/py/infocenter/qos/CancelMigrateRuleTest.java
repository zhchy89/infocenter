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
import py.icshare.qos.MigrationRuleInformation;
import py.icshare.qos.MigrationRuleStore;
import py.icshare.qos.MigrationRuleStatus;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.service.InformationCenterImpl;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.*;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CancelMigrateRuleTest extends TestBase {
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
     * A test for cancel MigrationRule.
     *
     * @throws InvalidInputException_Thrift
     * @throws TException
     */
    @Test
    public void testCancelMigrateSpeed() throws Exception{
        CancelMigrationRulesRequest request = new CancelMigrationRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(100000);
        request.addToStoragePoolIds(1L);
        request.setRuleId(1L);
        request.setCommit(true);

        when(migrationRuleStore.get(eq(1L))).thenReturn(ApplyMigrateRuleTest.buildMigrateRuleInformation(1L, MigrationRuleStatus.AVAILABLE));

        when(storagePoolStore.getStoragePool(eq(1L))).thenReturn(buildStoragePool(1L));

        icImpl.cancelMigrationRules(request);

        Mockito.verify(storagePoolStore, Mockito.times(1)).saveStoragePool(any(StoragePool.class));
    }




    List<StoragePool> buildStoragePoolList() {
        List<StoragePool> pools = new ArrayList<StoragePool>();
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
        storagePool1.setMigrationRuleId(1L);
        StoragePool storagePool2 = new StoragePool();
        storagePool2.setDomainId(domainId);
        storagePool2.setPoolId(storagePoolId2);
        storagePool2.setName(storagePoolName2);
        storagePool2.setMigrationRuleId(2L);

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

        return storagePool;
    }


}

