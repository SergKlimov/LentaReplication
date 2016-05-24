package ru.spbstu.kspt;

import spark.*;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Statistics extends TimerTask implements TemplateViewRoute {
    static ConcurrentMap<Integer, Shop> perShop = new ConcurrentHashMap<>();
    static AtomicInteger insertCount = new AtomicInteger();
    static AtomicInteger dupesCount = new AtomicInteger();

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
        long newValue = insertCount.get() + dupesCount.get();
        diff = newValue - oldValue;
        if (diff != 0) {
            System.out.println("Inserts: " + diff);
        }
        oldValue = newValue;
    }

    @Override
    public ModelAndView handle(Request request, Response response) throws Exception {
        Map<String, Object> attributes = new HashMap<>();
        Map<Integer, Shop> shops = new HashMap<>(perShop);
        attributes.put("perShop", shops);
        attributes.put("totalInsertsCount", insertCount.get());
        attributes.put("totalDupesCount", dupesCount.get());
        attributes.put("totalInsertsSpeed", diff);

        return new ModelAndView(attributes, "stats.html");
    }

    static public void update(int insertCounter, int dupesCounter, int store) {
        insertCount.addAndGet(insertCounter);
        dupesCount.addAndGet(dupesCounter);
        perShop.putIfAbsent(store, new Shop());
        perShop.get(store).insertCount++;
        perShop.get(store).lastRequest = new Date();
    }

    public static class Shop {
        public Date lastRequest;
        public long insertCount;
    }
}
