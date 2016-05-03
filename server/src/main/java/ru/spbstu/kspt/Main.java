/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
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
import org.thymeleaf.templateresolver.FileTemplateResolver;
import spark.template.thymeleaf.ThymeleafTemplateEngine;

import java.io.File;
import java.io.IOException;
import java.util.Timer;

import static spark.Spark.get;
import static spark.Spark.post;

/**
 * @author ann
 */

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

    private static HikariDataSource setUpDataSource(Config config) {
        HikariConfig dbConfig = new HikariConfig();
        dbConfig.setJdbcUrl(config.getDb().getUrl());
        dbConfig.setUsername(config.getDb().getUser());
        dbConfig.setPassword(config.getDb().getPass());
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

        HikariDataSource ds = setUpDataSource(config);

        Sql2o sql2o = new Sql2o(ds, new PostgresQuirks());
        createTable(sql2o);

        // TODO: Use threadPool(300);

        Push push = new Push(sql2o);
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
            sql2o.open().createQuery("VACUUM FULL ANALYZE");
            return "";
        });
    }
}


