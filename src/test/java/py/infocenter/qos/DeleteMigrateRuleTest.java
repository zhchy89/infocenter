package py.infocenter.qos;

import junit.framework.Assert;
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
import py.icshare.qos.*;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.service.InformationCenterImpl;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.DeleteMigrationRulesRequest;
import py.thrift.share.DeleteMigrationRulesResponse;
import py.thrift.share.MigrationRule_Thrift;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeleteMigrateRuleTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(DeleteMigrateRuleTest.class);

    private InformationCenterImpl icImpl;

    @Mock
    private InfoCenterAppContext appContext;

    @Mock
    private MigrationRuleStore migrationRuleStore;

    @Mock
    private StoragePoolStore storagePoolStore;

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
     * A test for deleting rules with false commit field. In the case, no rules could be delete.
     * @throws Exception
     */
    @Test
    public void testDeleteMigrateSpeed() throws Exception {
        DeleteMigrationRulesRequest request = new DeleteMigrationRulesRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setAccountId(1111);
        request.addToRuleIds(1L);
        request.addToRuleIds(2L);
        request.addToRuleIds(3L);
        request.commit = true;

        List<StoragePool> pools = buildStoragePoolList();

        when(storagePoolStore.listAllStoragePools()).thenReturn(pools);
        when(migrationRuleStore.get(eq(1L))).thenReturn(ApplyMigrateRuleTest.buildMigrateRuleInformation(1L, MigrationRuleStatus.DELETING));
        when(migrationRuleStore.get(eq(2L))).thenReturn(ApplyMigrateRuleTest.buildMigrateRuleInformation(2L, MigrationRuleStatus.AVAILABLE));
        when(migrationRuleStore.get(eq(3L))).thenReturn(ApplyMigrateRuleTest.buildMigrateRuleInformation(3L, MigrationRuleStatus.AVAILABLE));
        when(migrationRuleStore.get(eq(4L))).thenReturn(ApplyMigrateRuleTest.buildMigrateRuleInformation(4L, MigrationRuleStatus.AVAILABLE));
        when(migrationRuleStore.get(eq(5L))).thenReturn(ApplyMigrateRuleTest.buildMigrateRuleInformation(5L, MigrationRuleStatus.AVAILABLE));

        DeleteMigrationRulesResponse response = icImpl.deleteMigrationRules(request);


        Mockito.verify(migrationRuleStore, Mockito.times(2)).delete(anyLong());

        logger.error("response {}", response);
        for (MigrationRule_Thrift ruleFromRemote: response.getAirMigrationRuleList()) {
            long ruleId = ruleFromRemote.getRuleId();
            Assert.assertTrue(ruleId == 1L );
        }
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
        StoragePool storagePool2 = new StoragePool();
        storagePool2.setDomainId(domainId);
        storagePool2.setPoolId(storagePoolId2);
        storagePool2.setName(storagePoolName2);
        storagePool2.setMigrationRuleId(2L);

        pools.add(storagePool1);
        pools.add(storagePool2);

        return pools;
    }



}