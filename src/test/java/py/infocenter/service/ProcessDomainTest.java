package py.infocenter.service;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;

import py.app.context.AppContext;
import py.archive.RawArchiveMetadata;
import py.archive.ArchiveStatus;
import py.archive.StorageType;
import py.common.RequestIdBuilder;
import py.common.client.RequestResponseHelper;
import py.icshare.Domain;
import py.icshare.DomainStore;
import py.icshare.InstanceMetadata;
import py.icshare.StoragePool;
import py.icshare.StoragePoolStore;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStore;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.CreateDomainRequest;
import py.thrift.share.DatanodeNotFoundException_Thrift;
import py.thrift.share.DatanodeNotFreeToUseException_Thrift;
import py.thrift.share.DeleteDomainRequest;
import py.thrift.share.DomainExistedException_Thrift;
import py.thrift.share.DomainNameExistedException_Thrift;
import py.thrift.share.DomainNotExistedException_Thrift;
import py.thrift.share.Domain_Thrift;
import py.thrift.share.InvalidInputException_Thrift;
import py.thrift.share.ListDomainRequest;
import py.thrift.share.ListDomainResponse;
import py.thrift.share.RemoveDatanodeFromDomainRequest;
import py.thrift.share.Status_Thrift;
import py.thrift.share.StillHaveStoragePoolException_Thrift;
import py.thrift.share.UpdateDomainRequest;

public class ProcessDomainTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(ProcessDomainTest.class);

    @Mock
    private StorageStore storageStore;
    @Mock
    private VolumeStore volumeStore;
    @Mock
    private DomainStore domainStore;
    @Mock
    private StoragePoolStore storagePoolStore;
    private InformationCenterImpl icImpl;

    @Before
    public void init() throws Exception {
        super.init();
        InfoCenterAppContext appContext = mock(InfoCenterAppContext.class);
        when(appContext.getStatus()).thenReturn(InstanceStatus.OK);

        icImpl = new InformationCenterImpl();
        icImpl.setStorageStore(storageStore);
        icImpl.setVolumeStore(volumeStore);
        icImpl.setDomainStore(domainStore);
        icImpl.setStoragePoolStore(storagePoolStore);
        icImpl.setAppContext(appContext);
    }

    @Test
    public void testCreateDomainSucess() throws TException {
        CreateDomainRequest request = new CreateDomainRequest();
        request.setRequestId(RequestIdBuilder.get());
        Domain_Thrift domainThrift = new Domain_Thrift();
        domainThrift.setDomainId(RequestIdBuilder.get());
        domainThrift.setDomainName(TestBase.getRandomString(10));
        domainThrift.setDomainDescription(TestBase.getRandomString(20));
        domainThrift.setStatus(Status_Thrift.Available);
        domainThrift.setLastUpdateTime(System.currentTimeMillis());
        Set<Long> datanodes = buildIdSet(5);
        domainThrift.setDatanodes(datanodes);
        request.setDomain(domainThrift);

        InstanceMetadata datanode = mock(InstanceMetadata.class);

        for (Long datanodeId : datanodes) {
            when(storageStore.get(datanodeId)).thenReturn(datanode);
        }
        when(datanode.isFree()).thenReturn(true);
        icImpl.createDomain(request);

        Mockito.verify(domainStore, Mockito.times(1)).saveDomain(any(Domain.class));
    }

    @Test
    public void testCreateDomainThrowException1() throws TException, Exception {
        CreateDomainRequest requestSucess = new CreateDomainRequest();
        requestSucess.setRequestId(RequestIdBuilder.get());
        Domain_Thrift domainThrift = new Domain_Thrift();
        Long sucessDomainId = RequestIdBuilder.get();
        String successDomainName = TestBase.getRandomString(10);
        domainThrift.setDomainId(sucessDomainId);
        domainThrift.setDomainName(successDomainName);
        domainThrift.setDomainDescription(TestBase.getRandomString(20));
        domainThrift.setStatus(Status_Thrift.Available);
        domainThrift.setLastUpdateTime(System.currentTimeMillis());
        Set<Long> datanodes = buildIdSet(5);
        domainThrift.setDatanodes(datanodes);
        requestSucess.setDomain(domainThrift);

        InstanceMetadata datanode = mock(InstanceMetadata.class);

        for (Long datanodeId : datanodes) {
            when(storageStore.get(datanodeId)).thenReturn(datanode);
        }
        when(datanode.isFree()).thenReturn(true);
        icImpl.createDomain(requestSucess);

        Mockito.verify(domainStore, Mockito.times(1)).saveDomain(any(Domain.class));

        // domain name too long
        CreateDomainRequest requestNameTooLong = new CreateDomainRequest();
        requestNameTooLong.setRequestId(RequestIdBuilder.get());
        Domain_Thrift domainThrift1 = new Domain_Thrift();
        domainThrift1.setDomainId(RequestIdBuilder.get());
        domainThrift1.setDomainName(TestBase.getRandomString(101));
        domainThrift1.setDomainDescription(TestBase.getRandomString(20));
        domainThrift1.setStatus(Status_Thrift.Available);
        domainThrift1.setLastUpdateTime(System.currentTimeMillis());
        domainThrift1.setDatanodes(datanodes);
        requestNameTooLong.setDomain(domainThrift1);
        boolean caughtNameException = false;
        try {
            icImpl.createDomain(requestNameTooLong);
        } catch (InvalidInputException_Thrift e) {
            caughtNameException = true;
        }
        assertTrue(caughtNameException);

        List<Domain> allDomains = new ArrayList<Domain>();
        allDomains.add(RequestResponseHelper.buildDomainFrom(domainThrift));
        when(domainStore.listAllDomains()).thenReturn(allDomains);

        // domain name exist
        CreateDomainRequest requestNameExist = new CreateDomainRequest();
        requestNameExist.setRequestId(RequestIdBuilder.get());
        Domain_Thrift domainThrift2 = new Domain_Thrift();
        domainThrift2.setDomainId(RequestIdBuilder.get());
        domainThrift2.setDomainName(successDomainName);
        domainThrift2.setDomainDescription(TestBase.getRandomString(20));
        domainThrift2.setDatanodes(datanodes);
        domainThrift2.setStatus(Status_Thrift.Available);
        domainThrift2.setLastUpdateTime(System.currentTimeMillis());
        requestNameExist.setDomain(domainThrift2);
        boolean caughtNameExistException = false;
        try {
            icImpl.createDomain(requestNameExist);
        } catch (DomainNameExistedException_Thrift e) {
            caughtNameExistException = true;
        }
        assertTrue(caughtNameExistException);

        // domain Id exist
        CreateDomainRequest requestDomainExist = new CreateDomainRequest();
        requestDomainExist.setRequestId(RequestIdBuilder.get());
        Domain_Thrift domainThrift3 = new Domain_Thrift();
        domainThrift3.setDomainId(sucessDomainId);
        domainThrift3.setDomainName(TestBase.getRandomString(99));
        domainThrift3.setDomainDescription(TestBase.getRandomString(20));
        domainThrift3.setDatanodes(datanodes);
        domainThrift3.setStatus(Status_Thrift.Available);
        domainThrift3.setLastUpdateTime(System.currentTimeMillis());
        requestDomainExist.setDomain(domainThrift3);
        boolean caughtDomainExistException = false;
        try {
            icImpl.createDomain(requestDomainExist);
        } catch (DomainExistedException_Thrift e) {
            caughtDomainExistException = true;
        }
        assertTrue(caughtDomainExistException);
    }

    @Test
    public void testCreateDomainThrowException2() throws TException {
        CreateDomainRequest requestNotFree = new CreateDomainRequest();
        requestNotFree.setRequestId(RequestIdBuilder.get());
        Domain_Thrift domainThrift = new Domain_Thrift();
        domainThrift.setDomainId(RequestIdBuilder.get());
        domainThrift.setDomainName(TestBase.getRandomString(10));
        domainThrift.setDomainDescription(TestBase.getRandomString(20));
        domainThrift.setStatus(Status_Thrift.Available);
        domainThrift.setLastUpdateTime(System.currentTimeMillis());
        Set<Long> datanodes = buildIdSet(5);
        domainThrift.setDatanodes(datanodes);
        requestNotFree.setDomain(domainThrift);

        InstanceMetadata datanode = mock(InstanceMetadata.class);

        for (Long datanodeId : datanodes) {
            when(storageStore.get(datanodeId)).thenReturn(datanode);
        }
        when(datanode.isFree()).thenReturn(false);
        boolean caughtNotFreeException = false;
        try {
            icImpl.createDomain(requestNotFree);
        } catch (DatanodeNotFreeToUseException_Thrift e) {
            caughtNotFreeException = true;
        }
        assertTrue(caughtNotFreeException);

        for (Long datanodeId : datanodes) {
            when(storageStore.get(datanodeId)).thenReturn(null);
        }
        boolean caughtNotFoundException = false;
        try {
            icImpl.createDomain(requestNotFree);
        } catch (DatanodeNotFoundException_Thrift e) {
            caughtNotFoundException = true;
        }
        assertTrue(caughtNotFoundException);
    }

    @Test
    public void testUpdateDomainSuccess() throws TException, Exception {
        // create first
        CreateDomainRequest requestSucess = new CreateDomainRequest();
        requestSucess.setRequestId(RequestIdBuilder.get());
        Domain_Thrift domainThrift = new Domain_Thrift();
        Long sucessDomainId = RequestIdBuilder.get();
        String successDomainName = TestBase.getRandomString(10);
        domainThrift.setDomainId(sucessDomainId);
        domainThrift.setDomainName(successDomainName);
        domainThrift.setDomainDescription(TestBase.getRandomString(20));
        domainThrift.setStatus(Status_Thrift.Available);
        domainThrift.setLastUpdateTime(System.currentTimeMillis());
        Set<Long> datanodes = buildIdSet(5);
        domainThrift.setDatanodes(datanodes);
        requestSucess.setDomain(domainThrift);

        InstanceMetadata datanode = mock(InstanceMetadata.class);

        for (Long datanodeId : datanodes) {
            when(storageStore.get(datanodeId)).thenReturn(datanode);
        }
        when(datanode.isFree()).thenReturn(true);
        icImpl.createDomain(requestSucess);

        Mockito.verify(domainStore, Mockito.times(1)).saveDomain(any(Domain.class));

        when(domainStore.getDomain(any(Long.class))).thenReturn(RequestResponseHelper.buildDomainFrom(domainThrift));
        // then update
        UpdateDomainRequest updateSucess = new UpdateDomainRequest();
        updateSucess.setRequestId(RequestIdBuilder.get());
        Domain_Thrift domainThrift1 = new Domain_Thrift();
        domainThrift1.setDomainId(sucessDomainId);
        domainThrift1.setDomainName(TestBase.getRandomString(10));
        domainThrift1.setDomainDescription(TestBase.getRandomString(20));
        domainThrift1.setDatanodes(datanodes);
        domainThrift1.setStatus(Status_Thrift.Available);
        domainThrift1.setLastUpdateTime(System.currentTimeMillis());
        updateSucess.setDomain(domainThrift1);

        icImpl.updateDomain(updateSucess);

        Mockito.verify(domainStore, Mockito.times(2)).saveDomain(any(Domain.class));
    }

    @Test
    public void testUpdateDomainThrowException() throws TException, Exception {
        // create first
        CreateDomainRequest requestSucess = new CreateDomainRequest();
        requestSucess.setRequestId(RequestIdBuilder.get());
        Domain_Thrift domainThrift = new Domain_Thrift();
        Long sucessDomainId = RequestIdBuilder.get();
        String successDomainName = TestBase.getRandomString(10);
        domainThrift.setDomainId(sucessDomainId);
        domainThrift.setDomainName(successDomainName);
        domainThrift.setDomainDescription(TestBase.getRandomString(20));
        domainThrift.setStatus(Status_Thrift.Available);
        domainThrift.setLastUpdateTime(System.currentTimeMillis());
        Set<Long> datanodes = buildIdSet(5);
        domainThrift.setDatanodes(datanodes);
        requestSucess.setDomain(domainThrift);

        InstanceMetadata datanode = mock(InstanceMetadata.class);

        for (Long datanodeId : datanodes) {
            when(storageStore.get(datanodeId)).thenReturn(datanode);
        }
        when(datanode.isFree()).thenReturn(true);
        icImpl.createDomain(requestSucess);

        Mockito.verify(domainStore, Mockito.times(1)).saveDomain(any(Domain.class));

        when(domainStore.getDomain(any(Long.class))).thenReturn(RequestResponseHelper.buildDomainFrom(domainThrift));
        // then update
        UpdateDomainRequest updateNotFree = new UpdateDomainRequest();
        updateNotFree.setRequestId(RequestIdBuilder.get());
        Domain_Thrift domainThrift1 = new Domain_Thrift();
        domainThrift1.setDomainId(sucessDomainId);
        domainThrift1.setDomainName(TestBase.getRandomString(10));
        domainThrift1.setDomainDescription(TestBase.getRandomString(20));
        domainThrift1.setStatus(Status_Thrift.Available);
        domainThrift1.setLastUpdateTime(System.currentTimeMillis());
        Long addDatanodeId = RequestIdBuilder.get();
        datanodes.add(addDatanodeId);
        domainThrift1.setDatanodes(datanodes);
        updateNotFree.setDomain(domainThrift1);
        when(storageStore.get(addDatanodeId)).thenReturn(datanode);
        when(datanode.isFree()).thenReturn(false);

        boolean datanodeNotFreeException = false;
        try {
            icImpl.updateDomain(updateNotFree);
        } catch (DatanodeNotFreeToUseException_Thrift e) {
            datanodeNotFreeException = true;
        }
        assertTrue(datanodeNotFreeException);

        // then update, but datanode not found
        UpdateDomainRequest updateNotFound = new UpdateDomainRequest();
        updateNotFound.setRequestId(RequestIdBuilder.get());
        Domain_Thrift domainThrift2 = new Domain_Thrift();
        domainThrift2.setDomainId(sucessDomainId);
        domainThrift2.setDomainName(TestBase.getRandomString(10));
        domainThrift2.setDomainDescription(TestBase.getRandomString(20));
        domainThrift2.setStatus(Status_Thrift.Available);
        domainThrift2.setLastUpdateTime(System.currentTimeMillis());
        Long addDatanodeId2 = RequestIdBuilder.get();
        datanodes.add(addDatanodeId2);
        domainThrift2.setDatanodes(datanodes);
        updateNotFound.setDomain(domainThrift2);
        when(storageStore.get(addDatanodeId2)).thenReturn(null);
        when(datanode.isFree()).thenReturn(true);

        boolean datanodeNotFoundException = false;
        try {
            icImpl.updateDomain(updateNotFound);
        } catch (DatanodeNotFoundException_Thrift e) {
            datanodeNotFoundException = true;
        }
        assertTrue(datanodeNotFoundException);

        when(domainStore.getDomain(any(Long.class))).thenReturn(null);
        // then update
        UpdateDomainRequest updateNotFoundDomain = new UpdateDomainRequest();
        updateNotFoundDomain.setRequestId(RequestIdBuilder.get());
        Domain_Thrift domainThrift3 = new Domain_Thrift();
        domainThrift3.setDomainId(sucessDomainId);
        domainThrift3.setDomainName(TestBase.getRandomString(10));
        domainThrift3.setDomainDescription(TestBase.getRandomString(20));
        domainThrift3.setDatanodes(datanodes);
        domainThrift3.setStatus(Status_Thrift.Available);
        domainThrift3.setLastUpdateTime(System.currentTimeMillis());
        updateNotFoundDomain.setDomain(domainThrift3);
        when(storageStore.get(addDatanodeId)).thenReturn(datanode);
        when(datanode.isFree()).thenReturn(true);

        boolean domainNotFoundException = false;
        try {
            icImpl.updateDomain(updateNotFoundDomain);
        } catch (DomainNotExistedException_Thrift e) {
            domainNotFoundException = true;
        }
        assertTrue(domainNotFoundException);
    }

    @Test
    public void testDeleteDomainThrowException() throws TException, Exception {
        // delete domain but not exist
        DeleteDomainRequest deleteRequest = new DeleteDomainRequest();
        deleteRequest.setRequestId(RequestIdBuilder.get());
        deleteRequest.setDomainId(RequestIdBuilder.get());

        boolean domainNotFoundException = false;
        try {
            icImpl.deleteDomain(deleteRequest);
        } catch (DomainNotExistedException_Thrift e) {
            domainNotFoundException = true;
        }
        assertTrue(domainNotFoundException);

        // create domain
        CreateDomainRequest requestSucess = new CreateDomainRequest();
        requestSucess.setRequestId(RequestIdBuilder.get());
        Domain_Thrift domainThrift = new Domain_Thrift();
        Long sucessDomainId = RequestIdBuilder.get();
        String successDomainName = TestBase.getRandomString(10);
        domainThrift.setDomainId(sucessDomainId);
        domainThrift.setDomainName(successDomainName);
        domainThrift.setDomainDescription(TestBase.getRandomString(20));
        domainThrift.setStatus(Status_Thrift.Available);
        domainThrift.setLastUpdateTime(System.currentTimeMillis());
        Set<Long> datanodes = buildIdSet(5);
        domainThrift.setDatanodes(datanodes);
        requestSucess.setDomain(domainThrift);

        InstanceMetadata datanode = mock(InstanceMetadata.class);

        for (Long datanodeId : datanodes) {
            when(storageStore.get(datanodeId)).thenReturn(datanode);
        }
        when(datanode.isFree()).thenReturn(true);
        icImpl.createDomain(requestSucess);

        Mockito.verify(domainStore, Mockito.times(1)).saveDomain(any(Domain.class));
        Domain domain = mock(Domain.class);
        when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
        when(domain.hasStoragePool()).thenReturn(true);
        // delete domain, but still has storage pool
        DeleteDomainRequest deleteRequestStillHasStoragePool = new DeleteDomainRequest();
        deleteRequestStillHasStoragePool.setRequestId(RequestIdBuilder.get());
        deleteRequestStillHasStoragePool.setDomainId(sucessDomainId);

        boolean hasStoragePoolException = false;
        try {
            icImpl.deleteDomain(deleteRequest);
        } catch (StillHaveStoragePoolException_Thrift e) {
            hasStoragePoolException = true;
        }
        assertTrue(hasStoragePoolException);

        // delete again
        when(domain.hasStoragePool()).thenReturn(false);
        icImpl.deleteDomain(deleteRequest);
        Mockito.verify(domainStore, Mockito.times(2)).saveDomain(any(Domain.class));
    }

    @Test
    public void testListDomain() throws TException, Exception {
        int domainCount = 10;
        List<Domain> allDomains = new ArrayList<Domain>();
        List<Domain> partOfDomains = new ArrayList<Domain>();
        List<Long> partOfDomainIds = new ArrayList<Long>();
        for (int i = 0; i < domainCount; i++) {
            Domain_Thrift domainThrift = new Domain_Thrift();
            domainThrift.setDomainId(Long.valueOf(i));
            domainThrift.setDomainName(TestBase.getRandomString(10));
            domainThrift.setDomainDescription(TestBase.getRandomString(20));
            domainThrift.setStatus(Status_Thrift.Available);
            domainThrift.setLastUpdateTime(System.currentTimeMillis());
            Set<Long> datanodes = buildIdSet(5);
            domainThrift.setDatanodes(datanodes);
            Domain domain = RequestResponseHelper.buildDomainFrom(domainThrift);
            allDomains.add(domain);
            if (i % 2 == 0) {
                partOfDomainIds.add(Long.valueOf(i));
                partOfDomains.add(domain);
            }
        }
        when(domainStore.listAllDomains()).thenReturn(allDomains);
        when(domainStore.listDomains(partOfDomainIds)).thenReturn(partOfDomains);

        ListDomainRequest listAllDomainRequest = new ListDomainRequest();
        listAllDomainRequest.setRequestId(RequestIdBuilder.get());
        ListDomainResponse response = icImpl.listDomains(listAllDomainRequest);
        assertTrue(response.getDomainDisplays().size() == domainCount);

        ListDomainRequest listPartOfDomainRequest = new ListDomainRequest();
        listPartOfDomainRequest.setRequestId(RequestIdBuilder.get());
        listPartOfDomainRequest.setDomainIds(partOfDomainIds);
        ListDomainResponse response1 = icImpl.listDomains(listPartOfDomainRequest);
        assertTrue(response1.getDomainDisplays().size() == partOfDomains.size());
    }

    @Test
    public void testRemoveDatanodeFromDomain() throws TException, Exception {
        // create first
        CreateDomainRequest requestSucess = new CreateDomainRequest();
        requestSucess.setRequestId(RequestIdBuilder.get());
        Domain_Thrift domainThrift = new Domain_Thrift();
        Long sucessDomainId = RequestIdBuilder.get();
        String successDomainName = TestBase.getRandomString(10);
        domainThrift.setDomainId(sucessDomainId);
        domainThrift.setDomainName(successDomainName);
        domainThrift.setDomainDescription(TestBase.getRandomString(20));
        domainThrift.setStatus(Status_Thrift.Available);
        domainThrift.setLastUpdateTime(System.currentTimeMillis());
        Set<Long> datanodes = buildIdSet(5);
        domainThrift.setDatanodes(datanodes);
        requestSucess.setDomain(domainThrift);

        InstanceMetadata datanode = mock(InstanceMetadata.class);

        for (Long datanodeId : datanodes) {
            when(storageStore.get(datanodeId)).thenReturn(datanode);
        }
        when(datanode.isFree()).thenReturn(true);
        icImpl.createDomain(requestSucess);

        Mockito.verify(domainStore, Mockito.times(1)).saveDomain(any(Domain.class));

        when(domainStore.getDomain(any(Long.class))).thenReturn(RequestResponseHelper.buildDomainFrom(domainThrift));

        // datanode not found
        Long notExistDatanodeId = RequestIdBuilder.get();
        when(storageStore.get(notExistDatanodeId)).thenReturn(null);

        RemoveDatanodeFromDomainRequest requestRemoveDatanode = new RemoveDatanodeFromDomainRequest();
        requestRemoveDatanode.setRequestId(RequestIdBuilder.get());
        requestRemoveDatanode.setDomainId(sucessDomainId);
        requestRemoveDatanode.setDatanodeInstanceId(notExistDatanodeId);
        boolean notFoundDatanodeException = false;
        try {
            icImpl.removeDatanodeFromDomain(requestRemoveDatanode);
        } catch (DatanodeNotFoundException_Thrift e) {
            notFoundDatanodeException = true;
        }
        assertTrue(notFoundDatanodeException);

        // domian not exist
        when(domainStore.getDomain(any(Long.class))).thenReturn(null);
        RemoveDatanodeFromDomainRequest requestDomainNotExist = new RemoveDatanodeFromDomainRequest();
        requestDomainNotExist.setRequestId(RequestIdBuilder.get());
        requestDomainNotExist.setDomainId(sucessDomainId);
        requestDomainNotExist.setDatanodeInstanceId((Long) datanodes.toArray()[0]);
        boolean domainNotExistException = false;
        try {
            icImpl.removeDatanodeFromDomain(requestDomainNotExist);
        } catch (DomainNotExistedException_Thrift e) {
            domainNotExistException = true;
        }
        assertTrue(domainNotExistException);

        // sucess delete datanode from domain
        Long removeDatanodeId = (Long) datanodes.toArray()[0];
        when(domainStore.getDomain(any(Long.class))).thenReturn(RequestResponseHelper.buildDomainFrom(domainThrift));
        StoragePool storagePool = mock(StoragePool.class);
        List<StoragePool> storagePoolList = new ArrayList<StoragePool>();
        storagePoolList.add(storagePool);
        when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
        Multimap<Long, Long> archivesInDataNode = buildMultiMap(5);
        List<RawArchiveMetadata> archives = new ArrayList<RawArchiveMetadata>();
        for (int i = 0; i < 5; i++) {
            RawArchiveMetadata archive = new RawArchiveMetadata();
            archive.setArchiveId((long)i);
            archive.setStatus(ArchiveStatus.GOOD);
            archive.setStorageType(StorageType.SATA);
            archives.add(archive);
            archivesInDataNode.put(removeDatanodeId, Long.valueOf(i));
        }

        when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);

        when(datanode.getArchives()).thenReturn(archives);
        RemoveDatanodeFromDomainRequest requestRemoveDatanodeSuccess = new RemoveDatanodeFromDomainRequest();
        requestRemoveDatanodeSuccess.setRequestId(RequestIdBuilder.get());
        requestRemoveDatanodeSuccess.setDomainId(sucessDomainId);
        requestRemoveDatanodeSuccess.setDatanodeInstanceId(removeDatanodeId);
        icImpl.removeDatanodeFromDomain(requestRemoveDatanodeSuccess);

        Mockito.verify(storagePoolStore, Mockito.times(5)).saveStoragePool(any(StoragePool.class));
        Mockito.verify(domainStore, Mockito.times(2)).saveDomain(any(Domain.class));
    }
}
