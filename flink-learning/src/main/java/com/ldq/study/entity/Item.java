package com.ldq.study.entity;

public class Item {
    public String name;
    public String desp;
    public String code;
    public Integer price;
    public String createTime;

    //java,java,1001,20.3,2019-01-02
    //C++,C++,1002,90,2019-01-02
    //,java,1001,20.3,2019-01-02
    //,java,1001,20.3,2019-01-02
    //python,python,1005,65,2019-01-02
    //python,python,1005,65,2019-01-02
    //hadoop,hadoop,1006,100,2019-01-02
    //hadoop,hadoop,1006,100,2019-01-02
    //hadoop,hadoop,1006,100,2019-01-02
    //hadoop,hadoop,1006,100,2019-01-02
    //hadoop,hadoop,1006,100,2019-01-02
    //java,java,1001,20.3,2019-01-02

    //Too many fields referenced from an atomic type
    //必须有无参构造函数，否则报上面的错误
    public Item(){}

    public Item(String name, String desp, String code, int price, String createTime) {
        this.name = name;
        this.desp = desp;
        this.code = code;
        this.price = price;
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "Item{" +
                "name='" + name + '\'' +
                ", desp='" + desp + '\'' +
                ", code='" + code + '\'' +
                ", price=" + price +
                ", createTime='" + createTime + '\'' +
                '}';
    }
}
