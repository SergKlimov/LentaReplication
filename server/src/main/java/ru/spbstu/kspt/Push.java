package ru.spbstu.kspt;

import org.sql2o.Connection;
import org.sql2o.Query;
import org.sql2o.Sql2o;
import spark.Response;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class Push {
    private Sql2o sql2o;
    private String insertStatement;
    private Config config;
//    private final int paramsNum = 19;

    Push(Sql2o sql2o, Config config) {
        this.sql2o = sql2o;
        this.config = config;
        generateParams(config.getColumnNum());
    }

    private void generateParams(int paramsNum) {
        List<String> positionalParams = new ArrayList<>(paramsNum);
        for (int i = 1; i <= paramsNum; i++) {
            positionalParams.add(":p" + i);
        }
        String params = String.join(", ", positionalParams);
        insertStatement = "INSERT INTO CHK VALUES (" + params + ")";
    }

    String push(Payload payload, Response response) {
        try (Connection con = sql2o.beginTransaction()) {
            Query query = con.createQuery(insertStatement);

            for (List<Object> row: payload.checks) {
                for (int dateColumn: config.getDateColumns()) {
                    double timestamp = (double) row.get(dateColumn);
                    long date = (long) timestamp;
                    row.set(dateColumn, new java.sql.Date(date));
                }
                row.add(payload.srcStore);
                query.withParams(row.toArray()).executeUpdate();
            }
            con.commit();

            Statistics.insertCount.addAndGet(payload.checks.size());
            Statistics.insertStats.put(payload.srcStore, new Date());
            return "";
        } catch (Exception e) {
            e.printStackTrace();
            response.status(500);
            return "Error: " + e.toString();
        }
    }
}
