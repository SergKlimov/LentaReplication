/**
 * Created by Artyom on 22.04.16.
 */

package ru.spbstu.kspt;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
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
import static spark.Spark.threadPool;

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

    private static HikariDataSource setUpDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost/test");
        config.setUsername("test_user");
        config.setPassword("qwerty");
        return new HikariDataSource(config);
    }

    public static void main(String[] args) throws SQLException {
        new Timer().schedule(new CountInserts(), 0, 1000);

        HikariDataSource ds = setUpDataSource();

        Sql2o sql2o = new Sql2o(ds, new PostgresQuirks());
        // createTable(sql2o);
        sql2o.open().createQuery("DELETE FROM CHK").executeUpdate();

        // TODO: Use threadPool(300);

        post("/pushJSON", new JSONPush(sql2o));
        post("/push", new CBORPush(sql2o));
        get("/push", new BenchmarkPush(sql2o));
        get("/checkIndexes", new CheckIndexes(sql2o));
    }
}


abstract class Push implements Route {
    static AtomicInteger insertCount = new AtomicInteger();
    Sql2o sql2o;

    public Push(Sql2o sql2o) {
        this.sql2o = sql2o;
    }

    abstract Map<String, Object> getData(Request request) throws Exception;

    @Override
    public Object handle(Request request, Response response) {
        try (Connection con = sql2o.beginTransaction()) {
            Map<String, Object> map = getData(request);

            Query query = con.createQuery("INSERT INTO CHK VALUES (:number, :status)");
            map.forEach(query::addParameter);
            query.executeUpdate();
            con.commit();

            insertCount.incrementAndGet();
            return "";
        } catch (Exception e) {
            response.status(500);
            return "Error: " + e.toString();
        }
    }
}

class JSONPush extends Push {
    public JSONPush(Sql2o sql2o) {
        super(sql2o);
    }

    @Override
    Map<String, Object> getData(Request request) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        return mapper.readValue(request.bodyAsBytes(),
                new TypeReference<Map<String, Object>>() {});
    }
}


class CBORPush extends Push {
    public CBORPush(Sql2o sql2o) {
        super(sql2o);
    }

    @Override
    Map<String, Object> getData(Request request) throws Exception {
        CBORFactory f = new CBORFactory();
        ObjectMapper mapper = new ObjectMapper(f);

        return mapper.readValue(request.bodyAsBytes(),
                new TypeReference<Map<String, Object>>() {});
    }
}


class BenchmarkPush extends Push {
    public BenchmarkPush(Sql2o sql2o) {
        super(sql2o);
    }

    static AtomicInteger localCount = new AtomicInteger();

    @Override
    Map<String, Object> getData(Request request) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        int num = localCount.getAndIncrement();

        String json = String.format("{\"number\":%d,\"status\":\"JSON\"}", num);

        return mapper.readValue(json,
                new TypeReference<Map<String, Object>>() {});
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