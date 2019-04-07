package com.codekutter.genesis.pipelines.processors;

import lombok.Data;
import lombok.ToString;

import java.util.Date;
import java.util.List;

@Data
@ToString
public class DemoEntity {
    private String id;
    private String name;
    private Date dateTime;
    private EDemo active;
    private List<String> values;
}
