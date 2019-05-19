package deployTest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class Test {
    public static void main(String[] args) {
        Collection<TaskManagerLocation> taskManagerLocations = new ArrayList(3);
        ((ArrayList) taskManagerLocations).add(new TaskManagerLocation("slave3"));
        ((ArrayList) taskManagerLocations).add(new TaskManagerLocation("slave1"));
        ((ArrayList) taskManagerLocations).add(new TaskManagerLocation("slave2"));

        System.out.println("排序前：");
        for(TaskManagerLocation t:taskManagerLocations){
            System.out.println(t.getHostName());
        }

        Collections.sort((ArrayList)taskManagerLocations,new PreferredSourceLocationsComparator());
        System.out.println("排序后：");
        for(TaskManagerLocation t:taskManagerLocations){
            System.out.println(t.getHostName());
        }
    }
}
