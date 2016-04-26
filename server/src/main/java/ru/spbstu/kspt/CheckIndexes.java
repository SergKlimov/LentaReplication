package ru.spbstu.kspt;

import org.sql2o.Sql2o;
import spark.Request;
import spark.Response;
import spark.Route;

import java.util.List;

public class CheckIndexes implements Route {
    Sql2o sql2o;

    public CheckIndexes(Sql2o sql2o) {
        this.sql2o = sql2o;
    }

    @Override
    public Object handle(Request request, Response response) throws Exception {
        StringBuilder sb = new StringBuilder();
        List<Long> numbers = sql2o.open()
                .createQuery("SELECT NUMBER FROM CHK ORDER BY NUMBER")
                .executeScalarList(Long.class);
        for (int i = 0; i < numbers.size(); i++) {
            long num = numbers.get(i);
            if (num == i) {
                sb.append(String.format("OK! %d == %d\n", num, i));
            } else {
                sb.append(String.format("Error! %d != %d\n", num, i));
                return sb.append("Error!\n").toString();
            }
        }
        return sb.append("OK\n").toString();
    }
}
