package py.infocenter.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import py.archive.segment.SegmentUnitMetadata;
import py.utils.Utils;

/**
 * This class is just a container, which contain all segment units to check if their report time is timeout it use a
 * delay queue.
 * 
 * @author david
 *
 */

public class SegmentUnitTimeoutStoreImpl implements SegmentUnitTimeoutStore {
    private static final Logger logger = LoggerFactory.getLogger(SegmentUnitTimeoutStore.class);
    private DelayQueue<SegmentUnitTimeoutContext> timeOutQueue;
    private final long segUnitTimeoutInSecond; // timeout for segment unit report, currently it is 90s

    public SegmentUnitTimeoutStoreImpl(long timeout) {
        this.segUnitTimeoutInSecond = timeout;
        timeOutQueue = new DelayQueue<SegmentUnitTimeoutContext>();
    }

    /**
     * 
     * @param segUnit
     */
    @Override
    public void addSegmentUnit(SegmentUnitMetadata segUnit) {
        SegmentUnitTimeoutContext segunitTimeoutContext = new SegmentUnitTimeoutContext(segUnit,
                segUnitTimeoutInSecond);
        timeOutQueue.put(segunitTimeoutContext);
    }

    /**
     * 
     * @param volumes
     *            Volume whose segment unit is timeout
     * @return
     */
    @Override
    public int drainTo(Collection<Long> volumes) {
        int count = 0;
        List<SegmentUnitTimeoutContext> segmentUnitCollection = new ArrayList<>();
        int timeoutNum = timeOutQueue.drainTo(segmentUnitCollection);
        if (timeoutNum == 0) {
            return 0;
        }

        // check it really timeout
        long now = System.currentTimeMillis();
        for (SegmentUnitTimeoutContext segContext : segmentUnitCollection) {
            if (now - segContext.getSegUnit().getLastReported() > segUnitTimeoutInSecond * 1000) {
                // it is really timeout
                volumes.add(segContext.getSegUnit().getSegId().getVolumeId().getId());
                count++;
                logger.debug("segment is timeout: lastReportTime is {}, now is {}, timeout is {}, segmentunit is {}",
                        Utils.millsecondToString(segContext.getSegUnit().getLastReported()),
                        Utils.millsecondToString(now), this.segUnitTimeoutInSecond, segContext.getSegUnit());
            } else { // for the segment unit not timeout, put the delayQueue again
                segContext.resetExpiredTime();
                this.timeOutQueue.add(segContext);
            }
        }

        return count;
    }

    /**
     * Clear the segment unit data
     */
    @Override
    public void clear() {
        this.timeOutQueue.clear();
    }

}

class SegmentUnitTimeoutContext implements Delayed {
    /** the segment unit included */
    private SegmentUnitMetadata segUnit;
    /** timeoutInSecond, currently it is 90s **/
    private long timeoutInSecond;
    /** After the time of expiredTime, the segUnit will be timeout */
    private long expiredTime;
    /** In delayQueue time */
    private long inQueueTime;

    /**
     * @param segUnit:
     *            segment unit which need to check the timeout status;
     * @param timeoutInSecond:
     *            after the this time, the segment unit will be timeout;
     */
    public SegmentUnitTimeoutContext(SegmentUnitMetadata segUnit, long timeoutInSecond) {
        this.segUnit = segUnit;
        this.expiredTime = this.segUnit.getLastReported() + timeoutInSecond * 1000;
        this.timeoutInSecond = timeoutInSecond;
        this.inQueueTime = System.currentTimeMillis();// Record the in queue time for debug. When it is created, it is
                                                      // put the delay queue immediately;
    }

    public SegmentUnitMetadata getSegUnit() {
        return this.segUnit;
    }

    @Override
    public int compareTo(Delayed o) {
        if (null == o) {
            return 1;
        }

        if (o == this) {
            return 0;
        }

        SegmentUnitTimeoutContext other = (SegmentUnitTimeoutContext) o;
        // Compare the lastReported time, if the last report time is more large, the possible of segment unit report
        // timeout is more smaller
        long diff = (this.expiredTime - other.expiredTime);
        return (diff == 0l) ? 0 : (diff > 0l ? 1 : -1);
    }

    @Override
    public long getDelay(TimeUnit unit) {
        long now = System.currentTimeMillis();
        return unit.convert(expiredTime - now, TimeUnit.MILLISECONDS);
    }

    /**
     * According to lastReport time, reset the expire time in future;
     */
    public void resetExpiredTime() {
        this.expiredTime = this.segUnit.getLastReported() + timeoutInSecond * 1000L;
        this.inQueueTime = System.currentTimeMillis();
    }

    public long getInQueueTime() {
        return inQueueTime;
    }

}