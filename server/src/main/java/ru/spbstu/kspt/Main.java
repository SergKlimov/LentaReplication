/**
 * Created by artyom on 22.04.16.
 */

package ru.spbstu.kspt;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.sql2o.Connection;
import org.sql2o.Query;
import org.sql2o.Sql2o;
import org.sql2o.data.Table;
import org.sql2o.quirks.PostgresQuirks;
import spark.Request;
import spark.Response;
import spark.Route;

import java.sql.SQLException;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import static spark.Spark.get;
import static spark.Spark.post;

public class Main {
    private static void createTable(Sql2o sql2o) {
        String sql =
                "CREATE TABLE CHK (" +
                        "    number INT PRIMARY KEY," +
                        "    status VARCHAR(10) NOT NULL" +
                        ")";

        try (Connection con = sql2o.beginTransaction()) {
            con.createQuery(sql).executeUpdate();
            con.commit();
        }
    }

    public static void main(String[] args) throws SQLException {
        Timer timer = new Timer();
        timer.schedule(new CountInserts(), 0, 1000);

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost/test");
        config.setUsername("test_user");
        config.setPassword("qwerty");
        HikariDataSource ds = new HikariDataSource(config);


        Sql2o sql2o = new Sql2o(ds, new PostgresQuirks());
        // createTable(sql2o);
        sql2o.open().createQuery("DELETE FROM CHK").executeUpdate();
        Push push = new Push(sql2o);
        CheckIndexes checkIndexes = new CheckIndexes(sql2o);
        post("/push", push);
        get("/checkIndexes", checkIndexes);
    }
}


class Push implements Route {
    static AtomicInteger insertCount = new AtomicInteger();
    Sql2o sql2o;
//    String json = String.format("{\"number\":%d,\"status\":\"JSON\"}", num);


    public Push(Sql2o sql2o) {
        this.sql2o = sql2o;
    }

    @Override
    public Object handle(Request request, Response response) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        Map<String, Object> map = mapper.readValue(request.bodyAsBytes(),
                new TypeReference<Map<String, Object>>() {
                });

        String sql2 = "INSERT INTO CHK VALUES (:number, :status)";

        try (Connection con = sql2o.beginTransaction()) {
            Query query = con.createQuery(sql2);
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                query.addParameter(entry.getKey(), entry.getValue());
            }
            query.executeUpdate();
            con.commit();
        }

        insertCount.incrementAndGet();

        return "";
    }
}


class CheckIndexes implements Route {
    Sql2o sql2o;

    public CheckIndexes(Sql2o sql2o) {
        this.sql2o = sql2o;
    }

    @Override
    public Object handle(Request request, Response response) throws Exception {
        StringBuilder sb = new StringBuilder();
        Table table = sql2o.open()
                .createQuery("SELECT * FROM CHK ORDER BY NUMBER")
                .executeAndFetchTable();
        for (int i = 0; i < table.rows().size(); i++) {
            Integer num = table.rows().get(i).getInteger("NUMBER");
            if (num.equals(i)) {
                sb.append(String.format("OK! %d == %d\n", num, i));
            } else {
                sb.append(String.format("Error! %d != %d\n", num, i));
                return sb.append("Error!\n").toString();
            }
        }
        return sb.append("OK\n").toString();
    }
}

class CountInserts extends TimerTask {
    int oldValue = 0;

    public void run() {
        int newValue = Push.insertCount.get();
        int diff = newValue - oldValue;
        if (diff != 0) {
            System.out.println("Inserts: " + diff);
        }
        oldValue = newValue;
    }
}