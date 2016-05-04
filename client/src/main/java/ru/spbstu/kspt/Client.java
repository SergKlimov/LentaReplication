package ru.spbstu.kspt;

import com.squareup.okhttp.*;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Главный класс клиента
 */
class Client {
  private final OkHttpClient client = new OkHttpClient();
  private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

  private String serverAddress;
  private String portNumber;
  private int pauseTime;

  Client() {
    Properties properties = new Properties();
    String propFileName = "config.properties";

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

  void run() {
    DatabaseService databaseService = new DatabaseService();

    while (true) {
      try {
        String jsonString = databaseService.getLastUpdatesByJson();
        if (!jsonString.equals("")) {
          sendMessage(jsonString);
          databaseService.deleteLastSelectedObjects();
        }
        Thread.sleep(this.pauseTime);
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
