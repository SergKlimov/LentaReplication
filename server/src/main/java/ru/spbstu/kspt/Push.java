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
        generateParams(config.columns.num);
    }

    private void generateParams(int paramsNum) {
        List<String> positionalParams = new ArrayList<>(paramsNum);
        for (int i = 1; i < paramsNum; i++) {
            positionalParams.add(":p" + i);
        }
        String params = String.join(", ", positionalParams);
        insertStatement = String.format("INSERT INTO CHK VALUES (%s, :store);", params);
    }

    private boolean isAlreadyInserted(Connection con, Object row[]) {
        int idColumn = config.columns.id;
        long id = ((Number)row[idColumn]).longValue();
        Long res = con.createQuery("SELECT id FROM chk WHERE id = :id")
                .addParameter("id", id).executeScalar(Long.class);
        return (res != null);
    }

    private void fixDateColumns(Object row[]) {
        for (int dateColumn: config.columns.date) {
            Long timestamp = ((Number)row[dateColumn]).longValue();
            row[dateColumn] = new java.sql.Date(timestamp);
        }
    }

    String push(Payload payload, Response response) {
        try (Connection con = sql2o.beginTransaction()) {
            Query query = con.createQuery(insertStatement);
            int insertCounter = 0;
            int dupesCounter = 0;

            for (Object row[]: payload.checks) {
                if (isAlreadyInserted(con, row)) {
                    logger.debug("Skipping: {}", Arrays.toString(row));
                    dupesCounter++;
                } else {
                    logger.debug("Inserting: {}", Arrays.toString(row));
                    fixDateColumns(row);
                    query.withParams(row)
                            .addParameter("store", payload.srcStore)
                            .executeUpdate();
                    insertCounter++;
                }
            }
            con.commit();

            Statistics.insertCount.addAndGet(insertCounter);
            Statistics.dupesCount.addAndGet(dupesCounter);
            Statistics.insertStats.put(payload.srcStore, new Date());
            return "";
        } catch (Exception e) {
            logger.error("Push error: {}", e);
            response.status(500);
            return "Error: " + e.toString();
        }
    }
}
