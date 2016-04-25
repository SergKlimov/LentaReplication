/**
 * Created by Artyom on 22.04.16.
 */

package ru.spbstu.kspt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.sql2o.Connection;
import org.sql2o.Query;
import org.sql2o.Sql2o;
import org.sql2o.quirks.PostgresQuirks;
import spark.Request;
import spark.Response;
import spark.Route;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import static spark.Spark.get;
import static spark.Spark.post;

public class Main {
    private static void createTable(Sql2o sql2o) {
        String sql =
                "CREATE TABLE IF NOT EXISTS CHK (" +
                        "    number INT PRIMARY KEY," +
                        "    status VARCHAR(10) NOT NULL," +
                        "    id_store INTEGER" +
                        ")";

        try (Connection con = sql2o.beginTransaction()) {
            con.createQuery(sql).executeUpdate();
            con.commit();
        }
    }

    private static HikariDataSource setUpDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:hsqldb:db/test");
        config.setUsername("sa");
        config.setPassword("");
//        config.setJdbcUrl("jdbc:postgresql://localhost/test");
//        config.setUsername("test_user");
//        config.setPassword("qwerty");
        return new HikariDataSource(config);
    }

    public static void main(String[] args) throws SQLException {
        new Timer().schedule(new CountInserts(), 0, 1000);

        HikariDataSource ds = setUpDataSource();

        Sql2o sql2o = new Sql2o(ds, new PostgresQuirks());
        createTable(sql2o);
        sql2o.open().createQuery("DELETE FROM CHK").executeUpdate();

        // TODO: Use threadPool(300);

        Push push = new Push(sql2o);
        CBORFactory cborFactory = new CBORFactory();
        ObjectMapper jsonMapper = new ObjectMapper();
        ObjectMapper cborMapper = new ObjectMapper(cborFactory);


        post("/pushJSON", (request, response) -> {
            Payload payload = jsonMapper.readValue(request.bodyAsBytes(),
                    Payload.class);
            return push.push(payload, response);
        });
        post("/push", (request, response) -> {
            Payload payload = cborMapper.readValue(request.bodyAsBytes(),
                    Payload.class);
            return push.push(payload, response);
        });
        get("/checkIndexes", new CheckIndexes(sql2o));
    }
}


class Push {
    static AtomicInteger insertCount = new AtomicInteger();
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
        System.out.println(insertStatement);
    }

    public String push(Payload payload, Response response) {
        try (Connection con = sql2o.beginTransaction()) {
            Query query = con.createQuery(insertStatement);

            for (List<Object> row: payload.checks) {
                row.add(payload.srcStore);
                query.withParams(row.toArray()).executeUpdate();
            }
            con.commit();

            insertCount.addAndGet(payload.checks.size());
            return "";
        } catch (Exception e) {
            e.printStackTrace();
            response.status(500);
            return "Error: " + e.toString();
        }
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
        List<Long> numbers = sql2o.open()
                .createQuery("SELECT NUMBER FROM CHK ORDER BY NUMBER")
                .executeScalarList(Long.class);
        for (int i = 0; i < numbers.size(); i++) {
            long num = numbers.get(i);
            if (num == i) {
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