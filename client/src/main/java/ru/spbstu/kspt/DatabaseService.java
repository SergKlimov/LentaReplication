package ru.spbstu.kspt;

import org.codehaus.jackson.map.ObjectMapper;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Класс для общения с БД
 */
public class DatabaseService {

  private String db_url;
  private String db_user;
  private String dp_password;

  private String storeId;

  public DatabaseService() {
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

      this.db_url = properties.getProperty("db_url");
      this.db_user = properties.getProperty("db_user");
      this.dp_password = properties.getProperty("db_pass");
      this.storeId = properties.getProperty("store_id");

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

  public String getLastUpdatesByJson() {
    Sql2o sql2o = new Sql2o(this.db_url, this.db_user, this.dp_password);

    String sql = "SELECT NUMBER, VALUE FROM BUFFER_TABLE";

    String ret = "";

    try {
      Connection con = sql2o.open();
      List<Map<String,Object>> payloads = con.createQuery(sql)
          .executeAndFetchTable().asList();

      class ResultPayload {
        public List<List<Object>> checks;
        public String srcStore;

        public ResultPayload () {
          checks = new ArrayList<List<Object>>();
        }
      }

      ResultPayload resultPayload = new ResultPayload();

      for(Map<String,Object> map: payloads) {
        List<Object> bufList = new ArrayList<Object>();
        for (String key : map.keySet()) {
          bufList.add(map.get(key));
        }
        resultPayload.checks.add(bufList);
      }

      resultPayload.srcStore = this.storeId;

      ret = convertObjectToJson(resultPayload);
    } catch (Exception ex) {
      ex.printStackTrace();
    }

    return ret;
  }

  public String convertObjectToJson(Object object) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.writeValueAsString(object);
      //return buf.substring(0, buf.length() - 1) + ",\"srcStore\": " + this.storeId + "}";
    } catch (IOException e) {
      e.printStackTrace();
    }
    return "";
  }
}
