package ru.spbstu.kspt;

import java.util.Arrays;
import java.util.List;

/**
 * Created by artyom on 26.04.16.
 */
class Config {
    private String DBUrl;
    private String DBUsername;
    private String DBPassword;
    private int columnNum;
    private int[] dateColumns;

    public String getDBUrl() {
        return DBUrl;
    }

    public void setDBUrl(String DBUrl) {
        this.DBUrl = DBUrl;
    }

    public String getDBUsername() {
        return DBUsername;
    }

    public void setDBUsername(String DBUsername) {
        this.DBUsername = DBUsername;
    }

    public String getDBPassword() {
        return DBPassword;
    }

    public void setDBPassword(String DBPassword) {
        this.DBPassword = DBPassword;
    }

    public int getColumnNum() {
        return columnNum;
    }

    public void setColumnNum(int columnNum) {
        this.columnNum = columnNum;
    }

    public int[] getDateColumns() {
        return dateColumns;
    }

    public void setDateColumns(int[] dateColumns) {
        this.dateColumns = dateColumns;
    }

    @Override
    public String toString() {
        return "Config{" +
                "DBUrl='" + DBUrl + '\'' +
                ", DBUsername='" + DBUsername + '\'' +
                ", DBPassword='" + DBPassword + '\'' +
                ", columnNum=" + columnNum +
                ", dateColumns=" + Arrays.toString(dateColumns) +
                '}';
    }
}
