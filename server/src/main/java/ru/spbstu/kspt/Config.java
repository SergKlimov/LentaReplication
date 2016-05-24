package ru.spbstu.kspt;

import java.util.Arrays;
import java.util.List;

/**
 * Created by artyom on 26.04.16.
 */
public class Config {
    public DB db;
    public Columns columns;

    public static class DB {
        public String url;
        public String username;
        public String password;

        @Override
        public String toString() {
            return "DB{" +
                    "url='" + url + '\'' +
                    ", username='" + username + '\'' +
                    ", password='" + password + '\'' +
                    '}';
        }
    }
    public static class Columns {
        public int num;
        public int[] date;
        public int id;
        public int id_store;

        @Override
        public String toString() {
            return "Columns{" +
                    "num=" + num +
                    ", date=" + Arrays.toString(date) +
                    ", id=" + id +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "Config{" +
                "db=" + db +
                ", columns=" + columns +
                '}';
    }
}
