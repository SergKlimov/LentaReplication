package ru.spbstu.kspt;

import java.util.TimerTask;

public class CountInserts extends TimerTask {
    long oldValue = 0;

    public void run() {
        long newValue = Push.insertCount.get();
        long diff = newValue - oldValue;
        if (diff != 0) {
            System.out.println("Inserts: " + diff);
        }
        oldValue = newValue;
    }
}
