package py.infocenter.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.common.VolumeMetadataJSONParser;
import py.instance.InstanceId;
import py.test.TestBase;
import py.test.TestUtils;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

import java.util.Date;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.*;
import static py.common.Constants.SUPERADMIN_ACCOUNT_ID;

/**
 * Created by yahuiliu on 13/04/2017.
 */
public class JsonPaserTest extends TestBase {

    @Test
    public void testVolumeMetadatatoJsonAndGetBack() {

        VolumeMetadata volumeMetadata = new VolumeMetadata(20000, 20001, 20002,
                20003, null, null, 0L, 0L);

        volumeMetadata.setVolumeId(37002);
        volumeMetadata.setRootVolumeId(1003);
        volumeMetadata.setChildVolumeId(null);
        volumeMetadata.setVolumeSize(1005);
        volumeMetadata.setExtendingSize(1006);
        volumeMetadata.setName("stdname");
        volumeMetadata.setVolumeType(VolumeType.REGULAR);
        volumeMetadata.setCacheType(CacheType.MEMORY);
        volumeMetadata.setVolumeStatus(VolumeStatus.Available);
        volumeMetadata.setAccountId(SUPERADMIN_ACCOUNT_ID);
        volumeMetadata.setSegmentSize(1008);
        volumeMetadata.setDeadTime(0L);
        volumeMetadata.setVolumeCreatedTime(new Date());
        volumeMetadata.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
        volumeMetadata.setReadWrite(VolumeMetadata.ReadWriteType.READ_WRITE);
        volumeMetadata.setPageWrappCount(128);
        volumeMetadata.setSegmentWrappCount(10);
        SegId segId = new SegId(37002, 11);
        SegmentMetadata segmentMetadata = new SegmentMetadata(segId, segId.getIndex());


        volumeMetadata.addSegmentMetadata(segmentMetadata, TestUtils.generateMembership());

        // build volumemetadata as a string
        ObjectMapper mapper = new ObjectMapper();
        String volumeString = null;
        try {
            volumeString = mapper.writeValueAsString(volumeMetadata);
        } catch (JsonProcessingException e) {
            logger.error("failed to build volumemetadata string ", e);
            fail();
        }

        System.out.println(volumeString);

        VolumeMetadataJSONParser parser = new VolumeMetadataJSONParser(volumeMetadata.getVersion(), volumeString);

        VolumeMetadata volumeFromJson;
        try {
            volumeFromJson = mapper.readValue(parser.getVolumeMetadataJSON(), VolumeMetadata.class);
            Date now = new Date();
            assertTrue("CreatedTime isn't close enough to the testTime!", now.getTime() - volumeFromJson.getVolumeCreatedTime().getTime() < 1000 * 60);
            assertEquals(VolumeMetadata.VolumeSourceType.CREATE_VOLUME, volumeFromJson.getVolumeSource());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void test() {
        Set<InstanceId> instanceIdList = new TreeSet<>((o1, o2) -> {
            int valCompare = Long.compare(0L, 0L);
            if (valCompare == 0) {
                return Integer.compare(o1.hashCode(), o2.hashCode());
            } else {
                return valCompare;
            }
        });
        InstanceId instanceId1 = new InstanceId(1L);
        InstanceId instanceId2 = new InstanceId(2L);
        instanceIdList.add(instanceId1);
        instanceIdList.add(instanceId2);
        logger.warn("1: {}, 2: {}", instanceId1.hashCode(), instanceId2.hashCode());
    }
}
