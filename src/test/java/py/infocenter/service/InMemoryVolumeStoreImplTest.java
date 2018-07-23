package py.infocenter.service;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import py.infocenter.store.*;
import py.volume.*;

public class InMemoryVolumeStoreImplTest{
	@Before
	public void init() {
	}
	@Test
	public void testInMemoryVolumeStoreImpl()
	{
		VolumeMetadata volumeMetadata = new VolumeMetadata();
		System.out.print("111");
		MemoryVolumeStoreImpl memoryVolumeStore = new MemoryVolumeStoreImpl();
		
		volumeMetadata.setName("aaa");
		volumeMetadata.setAccountId(11111);
		volumeMetadata.setVolumeId(11111);
		volumeMetadata.setVolumeStatus(VolumeStatus.Available);
		
		//test put and get normal is OK;
		memoryVolumeStore.saveVolume(volumeMetadata);
		VolumeMetadata volumeInDB = memoryVolumeStore.getVolume(11111L);
		Assert.assertNotNull(volumeInDB);
		Assert.assertEquals(volumeInDB.getAccountId(), 11111);
		Assert.assertEquals(volumeInDB.getName(), "aaa");
		Assert.assertEquals(volumeInDB.getVolumeStatus(), VolumeStatus.Available);
		
		volumeInDB = memoryVolumeStore.getVolume(11112L);
		Assert.assertNull(volumeInDB);
		
		memoryVolumeStore.deleteVolume(volumeMetadata);
		volumeInDB = memoryVolumeStore.getVolume(11111L);
		Assert.assertNull(volumeInDB);
		
		//test get volume not in dead status is OK
		memoryVolumeStore.saveVolume(volumeMetadata);
		volumeInDB = memoryVolumeStore.getVolumeNotDeadByName("aaa");
		Assert.assertNotNull(volumeInDB);
		
		volumeInDB = memoryVolumeStore.getVolumeNotDeadByName("AAA");
		Assert.assertNull(volumeInDB);
		
		volumeMetadata.setVolumeStatus(VolumeStatus.Dead);
		volumeInDB = memoryVolumeStore.getVolumeNotDeadByName("aaa");
		Assert.assertNull(volumeInDB);
			
		VolumeMetadata otherVolumeData = new VolumeMetadata();
		otherVolumeData.setName("aaa");
		otherVolumeData.setVolumeId(11111);
		otherVolumeData.setVolumeStatus(VolumeStatus.Available);
		memoryVolumeStore.saveVolume(otherVolumeData);
		volumeInDB = memoryVolumeStore.getVolumeNotDeadByName("aaa");
		Assert.assertTrue(volumeInDB == otherVolumeData);
		Assert.assertTrue(volumeInDB != volumeMetadata);
	}
}
