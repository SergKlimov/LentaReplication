package ru.spbstu.kspt;

import spark.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Statistics extends TimerTask implements TemplateViewRoute {
    static Map<Integer, Date> insertStats = new ConcurrentHashMap<>();
    static AtomicInteger insertCount = new AtomicInteger();

    long oldValue = 0;
    long diff;
    int period;

    Statistics(int period) {
        this.period = period;
    }

    int getPeriodInMillis() {
        return period*1000;
    }

    @Override
    public void run() {
        long newValue = insertCount.get();
        diff = newValue - oldValue;
        if (diff != 0) {
            System.out.println("Inserts: " + diff);
        }
        oldValue = newValue;
    }

    @Override
    public ModelAndView handle(Request request, Response response) throws Exception {
        Map<Integer, Date> map = new HashMap<>(insertStats);
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("stats", map);
        attributes.put("totalInsertsCount", insertCount.get());
        attributes.put("totalInsertsSpeed", diff);

        return new ModelAndView(attributes, "stats.html");
    }
}
