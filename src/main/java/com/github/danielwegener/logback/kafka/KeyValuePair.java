package com.github.danielwegener.logback.kafka;

public class KeyValuePair {

    public KeyValuePair() {}

    public KeyValuePair(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    private String key;
    private String value;

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public static KeyValuePair valueOf(String keyValue) {
        String[] split = keyValue.split("=", 2);
        if(split.length == 2) {
            return new KeyValuePair(split[0], split[1]);
        }
        return null;
    }

}
