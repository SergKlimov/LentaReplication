package ru.spbstu.kspt;

import com.squareup.okhttp.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Главный класс клиента
 */
public class Client {
  private final OkHttpClient client = new OkHttpClient();
  private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

  private String serverAddress;
  private String portNumber;
  private String storeId;
  private int pauseTime;

  public Client() {
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
      this.storeId = properties.getProperty("store_id");
      this.pauseTime = Integer.valueOf(properties.getProperty("pause_time"));

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        inputStream.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public void run() {

    //Тестовая строка для отправки
    String testString = "{\"checks\": [" +
        "[43, \"string1\"]" +
        "]," +
        "\"srcStore\": " + this.storeId + "}";

    try {
      sendMessage(testString);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void sendMessage(String jsonMes) throws Exception {
    Request request = new Request.Builder()
        .url("http://" + this.serverAddress + ":" + this.portNumber + "/pushJSON")
        .post(RequestBody.create(JSON, jsonMes))
        .build();

    Response response = client.newCall(request).execute();
    if (!response.isSuccessful())
      throw new IOException("Unexpected code " + response + "Body: " + response.body().string());
  }
}
