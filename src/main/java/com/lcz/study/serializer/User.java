package com.lcz.study.serializer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.util.Date;

@Data
@ToString
@AllArgsConstructor
public class User implements Serializable {

    private Integer id;

    private String name;

    private Date birthDate;

}
