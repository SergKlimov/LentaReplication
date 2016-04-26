package ru.spbstu.kspt;


import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class DatabaseServiceTest {
  @Test
  public void convertObjectToJson() throws Exception {
    Payload payload = new Payload();
    payload.checks = new ArrayList<Object>();

    List<Object> test1 = new ArrayList<Object>();
    test1.add("hello");
    test1.add("world1");

    List<Object> test2 = new ArrayList<Object>();
    test2.add("hello");
    test2.add("world2");

    payload.checks.add(test1);
    payload.checks.add(test2);

    DatabaseService databaseService = new DatabaseService();
    System.out.println(databaseService.convertObjectToJson(payload));
  }

}