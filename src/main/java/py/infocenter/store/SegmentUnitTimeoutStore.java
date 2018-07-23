package py.infocenter.store;

import java.util.Collection;

import py.archive.segment.SegmentUnitMetadata;

public interface SegmentUnitTimeoutStore {
	/*Add the segment unit for checking its report is timeout */
	public void addSegmentUnit(SegmentUnitMetadata segUnit);
	/*Get the volume ids whose segment unit is timeout */
	public int drainTo(Collection<Long> volumes);
	/*clear all segment unit which need to check status*/
	public void clear();
}
