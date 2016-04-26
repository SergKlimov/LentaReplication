/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.spbstu.kspt;

import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.sql2o.Sql2o;
import spark.Response;

/**
 *
 * @author ann
 */
class Compare {
    Sql2o sql2o;

    public Compare(Sql2o sql2o) {
        this.sql2o = sql2o;
    }

    private String generateDiff(List listFromRequest, List listFromDb) {
        Set s1 = new HashSet(listFromRequest);
        Set s2 = new HashSet(listFromDb);
        Set s3 = new HashSet(s1);
        s3.removeAll(s2);
        s2.removeAll(s1);
        s2.addAll(s3);  
        return s2.toString();
    }

    public String getDiff(PayloadForCompare payload, Response response) {
        List<Long> numbers = sql2o.open()
                .createQuery("SELECT*FROM CHK WHERE SRCID = "+ payload.srcStore)
                .executeScalarList(Long.class);
        return generateDiff(payload.checkIds, numbers);
    }
}

