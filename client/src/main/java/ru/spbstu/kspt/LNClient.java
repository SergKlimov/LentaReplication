package ru.spbstu.kspt;

import com.squareup.okhttp.*;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Created by ASER on 11.05.2016.
 */
class LNClient extends Thread {

    private Connection conn;
    private org.postgresql.PGConnection pgconn;

    private final OkHttpClient client = new OkHttpClient();
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    private final static String CONF_FILE = "config.properties";

    private String serverAddress;
    private String portNumber;
    private int pauseTime;

    LNClient(Connection conn) throws SQLException {
        configure(CONF_FILE);
        this.conn = conn;
        this.pgconn = (org.postgresql.PGConnection) conn;
        Statement stmt = conn.createStatement();
        stmt.execute("LISTEN mymessage");
        stmt.close();
    }

    private void configure(String conFile) {
        Properties properties = new Properties();
        String propFileName = conFile;

        InputStream inputStream = null;

        try {
            inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
            if (inputStream != null) {
                properties.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }

            this.serverAddress = properties.getProperty("server_address");
            this.portNumber = properties.getProperty("port_number");
            this.pauseTime = Integer.valueOf(properties.getProperty("pause_time"));

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        DatabaseService databaseService = new DatabaseService();
        while (true) {
            try {
                // issue a dummy query to contact the backend
                // and receive any pending notifications.
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT 1");
                rs.close();
                stmt.close();

                org.postgresql.PGNotification notifications[] = pgconn.getNotifications();
                if (notifications != null) {
                    String jsonString = databaseService.getAddedData(notifications.length);
                    if (!jsonString.equals("")) {
                        sendMessage(jsonString);
                        databaseService.deleteLastSelectedObjects();
                    }
                }
                // wait a while before checking again for new
                // notifications
                Thread.sleep(500);
            } catch (SQLException sqle) {
                sqle.printStackTrace();
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void sendMessage(String jsonMes) throws Exception {
        Request request = new Request.Builder()
                .url("http://" + this.serverAddress + ":" + this.portNumber + "/pushJSON")
                .post(RequestBody.create(JSON, jsonMes))
                .build();

        while (true) {
            Response response = client.newCall(request).execute();
            if (response.isSuccessful())
                break;
            else
                Thread.sleep(1000);
//        throw new IOException("Unexpected code " + response + "Body: " + response.body().string());
        }
    }

}
