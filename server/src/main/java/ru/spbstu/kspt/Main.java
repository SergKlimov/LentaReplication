package ru.spbstu.kspt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sql2o.Connection;
import org.sql2o.Sql2o;
import org.sql2o.quirks.PostgresQuirks;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;
import spark.template.thymeleaf.ThymeleafTemplateEngine;

import java.io.File;
import java.io.IOException;
import java.util.Timer;

import static spark.Spark.get;
import static spark.Spark.post;

public class Main {
    static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static void createTable(Sql2o sql2o) {
        String sql = "CREATE TABLE IF NOT EXISTS CHK (" +
                "id BIGINT," +
                "datecommit TIMESTAMP WITHOUT TIME ZONE," +
                "datecreate TIMESTAMP WITHOUT TIME ZONE," +
                "fiscaldocnum CHARACTER VARYING(64)," +
                "numberfield BIGINT," +
                "id_session BIGINT," +
                "id_shift BIGINT," +
                "checkstatus INTEGER," +
                "checksumend BIGINT," +
                "checksumstart BIGINT," +
                "discountvaluetotal BIGINT," +
                "operationtype BOOLEAN," +
                "receivedate TIMESTAMP WITHOUT TIME ZONE," +
                "id_purchaseref BIGINT," +
                "set5checknumber CHARACTER VARYING(64)," +
                "client_guid BIGINT," +
                "clienttype SMALLINT," +
                "denyprinttodocuments BOOLEAN," +
                "id_store INTEGER" +
                ");";

        try (Connection con = sql2o.beginTransaction()) {
            con.createQuery(sql).executeUpdate();
            con.commit();
        }
    }

    private static HikariDataSource setUpDataSource(Config config) {
        HikariConfig dbConfig = new HikariConfig();
        dbConfig.setJdbcUrl(config.db.url);
        dbConfig.setUsername(config.db.username);
        dbConfig.setPassword(config.db.password);
        return new HikariDataSource(dbConfig);
    }

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Main.class);
        Statistics stats = new Statistics(1);
        new Timer().schedule(stats, 0, stats.getPeriodInMillis());

        CBORFactory cborFactory = new CBORFactory();
        ObjectMapper jsonMapper = new ObjectMapper();
        ObjectMapper cborMapper = new ObjectMapper(cborFactory);

        Config config;
        try {
            config = jsonMapper.readValue(new File("config.json"),
                    Config.class);
        } catch (IOException e) {
            logger.error("Can't read config: {}", e.toString());
            return;
        }
        logger.info("Using config: {}", config);

        HikariDataSource ds = setUpDataSource(config);

        Sql2o sql2o = new Sql2o(ds, new PostgresQuirks());
        createTable(sql2o);

        // TODO: Use threadPool(300);

        Push push = new Push(sql2o, config);
        Compare comp = new Compare(sql2o);


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

        post("/compare", (request, response) -> {
            PayloadForCompare payload = cborMapper.readValue(request.bodyAsBytes(),
                    PayloadForCompare.class);
            return comp.getDiff(payload, response);
        });
        post("/compareJSON", (request, response) -> {
            PayloadForCompare payload = jsonMapper.readValue(request.bodyAsBytes(),
                    PayloadForCompare.class);
            return comp.getDiff(payload, response);
        });

        get("/stats", stats, new ThymeleafTemplateEngine(new ClassLoaderTemplateResolver()));

        post("/deleteAll", (request, response) -> {
            sql2o.open().createQuery("DELETE FROM CHK").executeUpdate();
            // sql2o.open().createQuery("VACUUM FULL ANALYZE");
            return "";
        });
    }
}


