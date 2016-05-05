/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.spbstu.kspt;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

/**
 *
 * @author ann
 */
public class PayloadForCompare {
    public PayloadForCompare(List<Long> checkIds, String srcStore) {
        this.srcStore = srcStore;
        this.checkIds = new ArrayList<Long>(checkIds);
    }
    public List<Long> checkIds;
    public String srcStore;
}