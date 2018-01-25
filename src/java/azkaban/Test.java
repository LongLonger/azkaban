package azkaban;

import azkaban.trigger.Trigger;
import azkaban.trigger.TriggerManager;

import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * @author zhongshu
 * @since 2017/12/22 下午4:37
 */
public class Test {

    public static void main(String[] args) {
        BlockingQueue<Trigger> triggers = new PriorityBlockingQueue<Trigger>(1, new TriggerComparator());
        Trigger t1 = new Trigger(6L);
        Trigger t5 = new Trigger(13L);
        Trigger t2 = new Trigger(9L);
        Trigger t3 = new Trigger(8L);
        Trigger t4 = new Trigger(5L);


        triggers.add(t1);
        triggers.add(t2);
        triggers.add(t3);
        triggers.add(t4);
        triggers.add(t5);

        for(Trigger t : triggers) {
            System.out.println("--> " + t.getNextCheckTime());
        }

    }
}


class TriggerComparator implements Comparator<Trigger> {
    @Override
    public int compare(Trigger arg0, Trigger arg1) {
        long first = arg1.getNextCheckTime();
        long second = arg0.getNextCheckTime();
        System.out.println("HEHA");
        if (first == second) {
            return 0;
        } else if (first < second) {
            return 1;
        }
        return -1;
    }
}