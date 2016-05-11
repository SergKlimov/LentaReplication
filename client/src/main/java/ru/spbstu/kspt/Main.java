package ru.spbstu.kspt;

import java.sql.Connection;
import java.sql.DriverManager;

public class Main {

  public static void main(String[] args) throws Exception{
    Client client = new Client();
    client.run();

    //run Listen - Notify client
    /*Class.forName("org.postgresql.Driver");
    String url = "jdbc:postgresql://localhost:5432/test";
    Connection lConn = DriverManager.getConnection(url,"postgres","pass");
    LNClient lnClient = new LNClient(lConn);
    lnClient.start();*/
  }
}
