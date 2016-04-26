package ru.spbstu.kspt;

import org.codehaus.jackson.map.ObjectMapper;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
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
      List<Payload> payloads = con.createQuery(sql)
          .executeScalarList(Payload.class);

      class ResultPayload {
        public List<List<Object>> resultPayloads;
        public String srcStore;

        public ResultPayload(List<Payload> payloads, String srcStore) {
          resultPayloads = new ArrayList<List<Object>>();

          for (Payload payload: payloads
               ) {
            resultPayloads.add(payload.checks);
          }
          this.srcStore = srcStore;
        }
      }

      ResultPayload resultPayload = new ResultPayload(payloads,this.storeId);
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
