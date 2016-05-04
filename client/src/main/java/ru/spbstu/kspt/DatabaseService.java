package ru.spbstu.kspt;

import org.codehaus.jackson.map.ObjectMapper;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Класс для общения с БД
 */
class DatabaseService {

  private String db_url;
  private String db_user;
  private String dp_password;
  private String table_name;

  private String storeId;

  private final List<String> rowList = new ArrayList<String>(Arrays.asList("id", "datecommit", "datecreate",
      "fiscaldocnum", "numberfield", "id_session", "id_shift", "checkstatus", "checksumend", "checksumstart",
      "discountvaluetotal", "operationtype", "receivedate", "id_purchaseref", "set5checknumber", "client_guid",
      "clienttype", "denyprinttodocuments"));

  private List<Object> lastIdList;

  public DatabaseService() {
    lastIdList = new ArrayList<Object>();
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
      this.table_name = properties.getProperty("table_name");

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

  String getLastUpdatesByJson() {
    Sql2o sql2o = new Sql2o(this.db_url, this.db_user, this.dp_password);

    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("SELECT ");

    for (int i = 0; i < rowList.size() - 1; i++) {
      stringBuilder.append(rowList.get(i))
          .append(", ");
    }

    stringBuilder.append(rowList.get(rowList.size() - 1))
        .append(" FROM ")
        .append(this.table_name);

    String sql = stringBuilder.toString();
    String ret = "";

    try {
      Connection con = sql2o.open();
      List<Map<String, Object>> payloads = con.createQuery(sql)
          .executeAndFetchTable().asList();

      if (payloads.isEmpty())
        return ret;

      class ResultPayload {
        public List<List<Object>> checks;
        public String srcStore;

        public ResultPayload() {
          checks = new ArrayList<List<Object>>();
        }
      }

      ResultPayload resultPayload = new ResultPayload();

      for (Map<String, Object> map : payloads) {
        List<Object> bufList = new ArrayList<Object>();
        for (String key : rowList) {
          bufList.add(map.get(key));
        }
        resultPayload.checks.add(bufList);
      }

      for (Map<String, Object> map : payloads) {
        lastIdList.add(map.get("id"));
      }

      resultPayload.srcStore = this.storeId;

      ret = convertObjectToJson(resultPayload);
    } catch (Exception ex) {
      ex.printStackTrace();
    }

    return ret;
  }

  String convertObjectToJson(Object object) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.writeValueAsString(object);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return "";
  }

  void deleteLastSelectedObjects() {
    Sql2o sql2o = new Sql2o(this.db_url, this.db_user, this.dp_password);
    String sql = "DELETE FROM " + this.table_name + " WHERE id = :id";
    for (Object id : lastIdList) {
      try {
        Connection con = sql2o.open();
        con.createQuery(sql)
            .addParameter("id", id)
            .executeUpdate();
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    lastIdList.clear();
  }
}
