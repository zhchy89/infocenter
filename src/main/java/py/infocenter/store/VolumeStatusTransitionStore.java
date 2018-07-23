package py.infocenter.store;

import java.util.Collection;
import py.volume.VolumeMetadata;

public interface VolumeStatusTransitionStore {
	public void addVolumeToStore(VolumeMetadata volume);
	public int drainTo(Collection <VolumeMetadata> c);
	public void clear();
}
