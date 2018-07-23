package py.infocenter.store;

import py.icshare.DiskInfo;
import java.util.List;

public interface DiskInfoStore {
    void clearDB();

    List<DiskInfo> listDiskInfos();

    DiskInfo listDiskInfoById(String id);

    void updateDiskInfoLightStatusById(String id, String status);
}
