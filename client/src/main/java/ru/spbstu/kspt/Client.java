package ru.spbstu.kspt;

import com.squareup.okhttp.*;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

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
    // проверка данных (метод CheckData вызывается каждый день в 24:00:00)
    Calendar calendar = Calendar.getInstance();
    calendar.set(Calendar.HOUR_OF_DAY, 24);
    calendar.set(Calendar.MINUTE, 00);
    calendar.set(Calendar.SECOND, 00);
    Date time = calendar.getTime();
    Timer timer = new Timer();
    timer.schedule(new CheckData(databaseService), time, TimeUnit.DAYS.toMillis(1));     
    
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
  
  public class CheckData extends TimerTask {
     DatabaseService databaseService;

    public CheckData(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    public void run() {
        try {
            String jsonString = databaseService.getAllChecksPerDayByJson();
            if (!jsonString.equals("")) {
                ArrayList<Integer> returnChecks = new ArrayList<Integer>(sendMessageCompare(jsonString));
                if(!returnChecks.isEmpty()) {
                    String jsonStringNew = databaseService.getEntryByIdByJson(returnChecks);
                    sendMessage(jsonStringNew);
                }
            } 
        } catch (Exception e) {
            e.printStackTrace();
        }        
    }
  }
  
  private ArrayList<Integer> sendMessageCompare(String jsonMes) throws Exception {
    Request request = new Request.Builder()
        .url("http://" + this.serverAddress + ":" + this.portNumber + "/compareJSON")
        .post(RequestBody.create(JSON, jsonMes))
        .build();
    while (true) {
        Response response = client.newCall(request).execute();
        if (response.isSuccessful()) {
            String responseStr = response.body().string().replace("[", "").replace("]", "").replace(",", "");
            Scanner scanner = new Scanner(responseStr);
            ArrayList<Integer> checkIds = new ArrayList<Integer>();
            while (scanner.hasNextInt()) 
                checkIds.add(scanner.nextInt());
            return checkIds;
        }      
        else
          Thread.sleep(1000);
    }
  }
}
