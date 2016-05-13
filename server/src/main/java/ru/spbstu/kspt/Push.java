package ru.spbstu.kspt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private final Logger logger = LoggerFactory.getLogger(Push.class);

    Push(Sql2o sql2o, Config config) {
        this.sql2o = sql2o;
        this.config = config;
        generateParams(config.getColumnNum());
    }

    private void generateParams(int paramsNum) {
        List<String> positionalParams = new ArrayList<>(paramsNum);
        for (int i = 1; i < paramsNum; i++) {
            positionalParams.add(":p" + i);
        }
        String params = String.join(", ", positionalParams);
        insertStatement = String.format("INSERT INTO CHK VALUES (%s, :store) " +
                "WHERE NOT EXISTS SELECT id FROM CHK WHERE id = :id);", params);
    }

    String push(Payload payload, Response response) {
        try (Connection con = sql2o.beginTransaction()) {
            Query query = con.createQuery(insertStatement);

            for (Object row[]: payload.checks) {
                logger.debug("Pushing row: ", Arrays.toString(row));
                for (int dateColumn: config.getDateColumns()) {
                    double timestamp = (double) row[dateColumn];
                    long date = (long) timestamp;
                    row[dateColumn] = new java.sql.Date(date);
                }
                query.withParams(row)
                        .addParameter("store", payload.srcStore)
                        .addParameter("id", row[0])
                        .executeUpdate();
            }
            con.commit();

            Statistics.insertCount.addAndGet(payload.checks.length);
            Statistics.insertStats.put(payload.srcStore, new Date());
            return "";
        } catch (Exception e) {
            e.printStackTrace();
            response.status(500);
            return "Error: " + e.toString();
        }
    }
}
