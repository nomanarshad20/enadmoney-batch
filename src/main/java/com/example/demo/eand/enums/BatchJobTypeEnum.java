package com.example.demo.eand.enums;

public enum BatchJobTypeEnum {
    PRE_INACTIVE_USER,
    INACTIVE_USER,
    DORMANT_USER;




    public static BatchJobTypeEnum fromString(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Value cannot be null or empty");
        }

        for (BatchJobTypeEnum type : BatchJobTypeEnum.values()) {
            if (type.name().equalsIgnoreCase(value.trim())) {
                return type;
            }
        }
        return null;
    }
}
