package com.ldq.study.utils;

import java.util.Locale;

import com.alibaba.fastjson.JSONObject;
import com.github.javafaker.Address;
import com.github.javafaker.Faker;
import com.ldq.study.entity.Person;

public class FakerUtils {
    private static Faker faker = new Faker(Locale.SIMPLIFIED_CHINESE);

    public void test() {
        System.out.println(faker.name().fullName());
        System.out.println(faker.book().title());
        System.out.println(faker.book().publisher());
    }


    public static Person randPerson() {
        Address address = faker.address();
        Person p = new Person();
        p.setName(faker.name().fullName());
        p.setAge(faker.number().numberBetween(10, 40));
        p.setSex(faker.number().numberBetween(0, 1));
        p.setAddress(address.secondaryAddress());
        p.setCity(address.city());
        p.setCountry("中国");
        p.setSalary(faker.number().randomDouble(2, 1000, 20000));
        System.out.println(p.toString());
        return p;
    }

    public static JSONObject getJsonPerson(){
        Person person = randPerson();
        JSONObject jsonObject = (JSONObject) JSONObject.toJSON(person);
        System.out.println("jsonObject = " + jsonObject);
        return jsonObject;
    }

    public static void main(String[] args) {
        getJsonPerson();
    }
}
