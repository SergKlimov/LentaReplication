package ru.spbstu.kspt;

import org.sql2o.Connection;
import org.sql2o.Query;
import org.sql2o.Sql2o;
import spark.Response;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Push {
    Sql2o sql2o;
    String insertStatement;
    final int paramsNum = 3;

    public Push(Sql2o sql2o) {
        this.sql2o = sql2o;
        generateParams();
    }

    private void generateParams() {
        List<String> positionalParams = new ArrayList<>(paramsNum);
        for (int i = 1; i <= paramsNum; i++) {
            positionalParams.add(":p" + i);
        }
        String params = String.join(", ", positionalParams);
        insertStatement = "INSERT INTO CHK VALUES (" + params + ")";
    }

    public String push(Payload payload, Response response) {
        try (Connection con = sql2o.beginTransaction()) {
            Query query = con.createQuery(insertStatement);

            for (List<Object> row: payload.checks) {
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
