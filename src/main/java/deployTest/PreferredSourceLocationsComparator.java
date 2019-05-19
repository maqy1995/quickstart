package deployTest;

import deployTest.TaskManagerLocation;

import java.util.Comparator;

public class PreferredSourceLocationsComparator implements Comparator<TaskManagerLocation> {
    @Override
    public int compare(TaskManagerLocation o1, TaskManagerLocation o2) {
        return o1.getHostName().compareTo(o2.getHostName());
    }
}
