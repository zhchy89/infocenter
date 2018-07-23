package py.infocenter.qos;

import net.sf.json.JSONArray;
import org.junit.Test;
import py.common.client.RequestResponseHelper;
import py.icshare.qos.IOLimitation;
import py.icshare.qos.IOLimitationEntry;
import py.instance.InstanceId;
import py.test.TestBase;
import py.utils.Utils;

import java.time.LocalTime;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ParseJsonIOLimitationEntryTest extends TestBase {
    @Test
    public void testParseJsonStrFromInstanceIdList() {
        List<InstanceId> datanodeIdList = new ArrayList<>();
        InstanceId id1 = new InstanceId(10001);
        InstanceId id2 = new InstanceId(10002);
        InstanceId id3 = new InstanceId(10003);
        datanodeIdList.add(id1);
        datanodeIdList.add(id2);
        datanodeIdList.add(id3);

        String trans = Utils.bulidJsonStrFromObject(datanodeIdList);

        Set<InstanceId> transList = new HashSet<>();
        transList.addAll(Utils.parseObjecFromJsonStr(trans));
        assertTrue(transList.size() == datanodeIdList.size());
        for (InstanceId datanodeId : datanodeIdList) {
            assertTrue(transList.contains(datanodeId));
        }
    }

    @Test
    public void testParseJsonIOLimitationEntry()  throws  Exception{
        List<IOLimitationEntry> entryList = new ArrayList<>();
        IOLimitationEntry entry1 = new IOLimitationEntry(1, 2, 1, 3, 2, LocalTime.parse("11:11:11"), LocalTime.parse("11:11:11"));
        entryList.add(entry1);

        JSONArray jsonArray = new JSONArray();
        for (IOLimitationEntry entry : entryList) {
            jsonArray.add(entry.toJsonString());
        }

        List<IOLimitationEntry> ioLimitationEntries = new ArrayList<>();
        logger.debug("parseIOLimitationEntries {}", jsonArray);
        Iterator<Object> iterator = jsonArray.iterator();
        while (iterator.hasNext()) {
            String json = iterator.next().toString();
            logger.warn("parseIOLimitationEntries json {}", json);
            ioLimitationEntries.add(IOLimitationEntry.fromJson(json));
            logger.warn("parseIOLimitationEntries after {}", ioLimitationEntries);
        }

        assertEquals(1, ioLimitationEntries.size());
    }


    @Test
    public void testJudgeDynamicIOLimitationTimeInterleaving() {
        IOLimitation updateIOLimitation = new IOLimitation();

        LocalTime startTime0 = LocalTime.now().plusSeconds(0);
        LocalTime endTime0 = LocalTime.now().plusSeconds(4);

        LocalTime startTime1 = LocalTime.now().plusSeconds(5);
        LocalTime endTime1 = LocalTime.now().plusSeconds(10);

        LocalTime startTime2 = LocalTime.now().plusSeconds(9);
        LocalTime endTime2 = LocalTime.now().plusSeconds(15);

        LocalTime startTime3 = LocalTime.now().plusSeconds(20);
        LocalTime endTime3 = LocalTime.now().plusSeconds(25);

        boolean existTimeInterleaving = RequestResponseHelper.judgeDynamicIOLimitationTimeInterleaving(updateIOLimitation);
        assertFalse(existTimeInterleaving);

        List<IOLimitationEntry> entries = new ArrayList<>();
        IOLimitationEntry entry1 = new IOLimitationEntry(0,100, 10, 1000, 100, startTime0, endTime0);
        IOLimitationEntry entry2 = new IOLimitationEntry(1,100, 10, 1000, 100, startTime1, endTime1);
        IOLimitationEntry entry3 = new IOLimitationEntry(2,200, 20, 2000, 200, startTime2, endTime2);
        IOLimitationEntry entry4 = new IOLimitationEntry(3,300, 30, 3000, 300, startTime3, endTime3);
        entries.add(entry1);
        entries.add(entry2);


        updateIOLimitation.setLimitType(IOLimitation.LimitType.Dynamic);
        updateIOLimitation.setEntries(entries);

        logger.warn("testJudgeIOLimitationTimeInterleaving {} ", updateIOLimitation);
        existTimeInterleaving = RequestResponseHelper.judgeDynamicIOLimitationTimeInterleaving(updateIOLimitation);
        assertFalse(existTimeInterleaving);


        updateIOLimitation.setLimitType(IOLimitation.LimitType.Dynamic);
        List<IOLimitationEntry> entrys2 = new ArrayList<>();
        entrys2.add(entry1);
        entrys2.add(entry2);
        entrys2.add(entry3);
        entrys2.add(entry4);
        updateIOLimitation.setEntries(entrys2);
        logger.warn("testJudgeIOLimitationTimeInterleaving {} ", updateIOLimitation);
        existTimeInterleaving = RequestResponseHelper.judgeDynamicIOLimitationTimeInterleaving(updateIOLimitation);
        assertTrue(existTimeInterleaving);


        IOLimitation updateIOLimitation2 =null;
        logger.warn("testJudgeIOLimitationTimeInterleaving {} ", updateIOLimitation);
        existTimeInterleaving = RequestResponseHelper.judgeDynamicIOLimitationTimeInterleaving(updateIOLimitation2);
        assertFalse(existTimeInterleaving);

        updateIOLimitation2 =new IOLimitation();
        logger.warn("testJudgeIOLimitationTimeInterleaving {} ", updateIOLimitation);
        existTimeInterleaving = RequestResponseHelper.judgeDynamicIOLimitationTimeInterleaving(updateIOLimitation2);
        assertFalse(existTimeInterleaving);
    }

    private boolean inTimeSpan(LocalTime startTime, LocalTime endTime) {
        LocalTime now = LocalTime.now();
        if (startTime.isAfter(endTime))
            return false;
        if (now.isBefore(startTime) || now.isAfter(endTime)) {
            return false;
        }
        return true;
    }



}
