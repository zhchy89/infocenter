package py.infocenter.service;

import java.util.Date;
import java.util.List;

import org.hibernate.HibernateException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import py.infocenter.store.VolumeStore;
import py.test.TestBase;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

import javax.validation.constraints.Null;

import static py.common.Constants.SUPERADMIN_ACCOUNT_ID;
import static py.volume.VolumeMetadata.VolumeInAction.NULL;

public class DatabaseVolumeStoreImplTest extends TestBase {
    VolumeStore volumeStore = null;
    private VolumeMetadata volumeMetadata = new VolumeMetadata();

    @Before
    public void init() throws Exception {
        super.init();
        @SuppressWarnings("resource")
        ApplicationContext ctx = new AnnotationConfigApplicationContext(InformationCenterAppConfigTest.class);
        volumeStore = (VolumeStore) ctx.getBean("dbVolumeStore");
        volumeStore.clearData();
    }

    @Test
    public void testGetVolumeNotDeadByNameInDB() {
        volumeMetadata.setVolumeId(37002);
        volumeMetadata.setName("stdname");
        volumeMetadata.setVolumeType(VolumeType.REGULAR);
        volumeMetadata.setCacheType(CacheType.MEMORY);
        volumeMetadata.setVolumeStatus(VolumeStatus.Available);
        volumeMetadata.setAccountId(SUPERADMIN_ACCOUNT_ID);
        volumeMetadata.setSegmentSize(1008);
        volumeMetadata.setVolumeCreatedTime(new Date());
        volumeMetadata.setLastExtendedTime(new Date());
        volumeMetadata.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
        volumeMetadata.setReadWrite(VolumeMetadata.ReadWriteType.READ_WRITE);
        volumeMetadata.setPageWrappCount(128);
        volumeMetadata.setSegmentWrappCount(10);
        volumeMetadata.setInAction(NULL);

        //test get a volumeMetadata from DB is OK
        volumeStore.saveVolume(volumeMetadata);
        VolumeMetadata volumeInDB = volumeStore.getVolumeNotDeadByName("stdname");
        Assert.assertNotNull(volumeInDB);

        volumeInDB = volumeStore.getVolumeNotDeadByName("name");
        Assert.assertNull(volumeInDB);

        //test get a volumeMetadata in dead status
        VolumeMetadata otherVolume = new VolumeMetadata();
        otherVolume.setVolumeId(371111);
        otherVolume.setName("aaaa");
        otherVolume.setVolumeType(VolumeType.REGULAR);
        otherVolume.setCacheType(CacheType.MEMORY);
        otherVolume.setVolumeStatus(VolumeStatus.Dead);
        otherVolume.setAccountId(1862755152385798555L);
        otherVolume.setSegmentSize(1008);
        volumeStore.saveVolume(otherVolume);
        volumeInDB = volumeStore.getVolumeNotDeadByName("aaaa");
        Assert.assertNull(volumeInDB);
    }

    @Test
    public void testDatabaseStore() {
        VolumeMetadata volume = new VolumeMetadata();
        volume.setVolumeId(111111);
        volume.setRootVolumeId(111111);
        volume.setName("first");
        volume.setVolumeType(VolumeType.REGULAR);
        volume.setCacheType(CacheType.MEMORY);
        volume.setVolumeStatus(VolumeStatus.Available);
        volume.setAccountId(1862755152385798555L);
        volume.setSegmentSize(1008);
        volume.setVolumeCreatedTime(new Date());
        volume.setLastExtendedTime(new Date());
        volume.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
        volume.setReadWrite(VolumeMetadata.ReadWriteType.READ_WRITE);
        volume.setPageWrappCount(128);
        volume.setSegmentWrappCount(10);
        volume.setInAction(NULL);

        VolumeMetadata volume1 = new VolumeMetadata();
        volume1.setVolumeId(222222);
        volume1.setName("second");
        volume1.setVolumeType(VolumeType.REGULAR);
        volume1.setCacheType(CacheType.MEMORY);
        volume1.setVolumeStatus(VolumeStatus.Dead);
        volume1.setAccountId(1862755152385798555L);
        volume1.setSegmentSize(1008);
        volume1.setVolumeCreatedTime(new Date());
        volume1.setLastExtendedTime(new Date());
        volume1.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
        volume1.setReadWrite(VolumeMetadata.ReadWriteType.READ_WRITE);
        volume1.setPageWrappCount(128);
        volume1.setSegmentWrappCount(10);
        volume1.setInAction(NULL);

        VolumeMetadata volume2 = new VolumeMetadata();
        volume2.setVolumeId(111111);
        volume2.setRootVolumeId(111111);
        volume2.setName("first");
        volume2.setVolumeType(VolumeType.LARGE);
        volume2.setCacheType(CacheType.MEMORY);
        volume2.setVolumeStatus(VolumeStatus.Available);
        volume2.setAccountId(1862755152385798555L);
        volume2.setSegmentSize(1008);
        volume2.setVolumeCreatedTime(new Date());
        volume2.setLastExtendedTime(new Date());
        volume2.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        volume2.setReadWrite(VolumeMetadata.ReadWriteType.READ_WRITE);
        volume2.setPageWrappCount(128);
        volume2.setSegmentWrappCount(10);
        volume2.setInAction(NULL);

        VolumeMetadata volume3 = new VolumeMetadata();
        volume3.setVolumeId(111111);
        volume3.setRootVolumeId(111111);
        volume3.setName("first");
        volume3.setVolumeType(VolumeType.LARGE);
        volume3.setCacheType(CacheType.MEMORY);
        volume3.setVolumeStatus(VolumeStatus.Available);
        volume3.setAccountId(1862755152385798555L);
        volume3.setSegmentSize(1008);
        volume3.setVolumeCreatedTime(new Date());
        volume3.setLastExtendedTime(new Date());
        volume3.setVolumeSource(VolumeMetadata.VolumeSourceType.CLONE_VOLUME);
        volume3.setReadWrite(VolumeMetadata.ReadWriteType.READ_WRITE);
        volume3.setPageWrappCount(128);
        volume3.setSegmentWrappCount(10);
        volume3.setInAction(NULL);

        //save volume
        volumeStore.saveVolume(volume);
        volumeStore.saveVolume(volume1);

        try {
            volumeStore.saveVolume(volume2);
        } catch (HibernateException e) {
            Assert.assertTrue(true);
        }

        //list volume
        List<VolumeMetadata> volumes = volumeStore.listVolumes();
        Assert.assertNotNull(volumes);
        Assert.assertEquals(volumes.size(), 2);

        //get volume
        VolumeMetadata volumeGet = null;
        volumeGet = volumeStore.getVolume(111111L);
        Assert.assertNotNull(volumeGet);

        //listRootVolumes
        List<VolumeMetadata> roots = volumeStore.listRootVolumes();
        Assert.assertEquals(roots.size(), 1);

        //updateDeadTime
        volumeStore.updateDeadTime(111111, 123456);
            volumeGet = volumeStore.getVolume(111111L);
        Assert.assertEquals(123456, volumeGet.getDeadTime());

        //updateStatus,change twice to make sure the value has been changed
        volumeStore.updateStatusAndVolumeInAction(111111, VolumeStatus.Available.toString(), "NULL");
        volumeStore.updateStatusAndVolumeInAction(111111, VolumeStatus.Unavailable.toString(), "NULL");
        volumeGet = volumeStore.getVolume(111111L);
        Assert.assertEquals(VolumeStatus.Unavailable, volumeGet.getVolumeStatus());

        //updateSizeNameType
        volumeStore
                .updatePersistedItems(111111, 123, "00000", "LARGE", null,
                        false, 0, 0, 0, new Date(), new Date(),
                        "CREATE_VOLUME", VolumeMetadata.ReadWriteType.READ_WRITE.name());
        volumeGet = volumeStore.getVolume(111111L);
        Assert.assertEquals(123, volumeGet.getVolumeSize());
        Assert.assertEquals("00000", volumeGet.getName());
        Assert.assertEquals("LARGE", volumeGet.getVolumeType().toString());
    }
    @After
    public void cleanUp() {
    }
}