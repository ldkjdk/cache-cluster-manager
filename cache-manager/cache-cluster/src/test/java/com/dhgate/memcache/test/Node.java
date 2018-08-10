package com.dhgate.memcache.test;

@SuppressWarnings("serial")
public class Node implements java.io.Serializable {

    String name;
    String address;
    int    value;
    byte[] buf;

    public Node(){
        name = "测试";
        address = "beijing";
        buf = new byte[50];
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public byte[] getBuf() {
        return buf;
    }

    public void setBuf(byte[] buf) {
        this.buf = buf;
    }
}
