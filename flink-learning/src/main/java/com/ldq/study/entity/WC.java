package com.ldq.study.entity;

public class WC {
    public String name;
    public Integer cnt;

    public WC() {
    }

    public WC(String name, Integer count) {
        this.name = name;
        this.cnt = count;
    }


    @Override
    public String toString() {
        return "WC{" +
                "name='" + name + '\'' +
                ", count=" + cnt +
                '}';
    }
}