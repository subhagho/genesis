package com.codekutter.genesis.pipelines.processors;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class DemoEntityFactory {
    public static DemoEntity create(int valueCount) {
        DemoEntity de = new DemoEntity();
        de.setId(UUID.randomUUID().toString());
        de.setDateTime(new Date(System.currentTimeMillis() - (10000 * valueCount)));

        List<String> values = new ArrayList<>();
        for (int ii = 0; ii < valueCount; ii++) {
            values.add(String.format("VALUE_%d", ii));
        }
        de.setValues(values);
        int n = (valueCount % EDemo.values().length);
        if (n == 0) {
            de.setActive(EDemo.Active);
        } else if (n == 1) {
            de.setActive(EDemo.InActive);
        } else if (n == 2) {
            de.setActive(EDemo.Deleted);
        } else if (n == 3) {
            de.setActive(EDemo.Unknown);
        }
        return de;
    }

    public static DemoEntity create(int valueCount, String date) {
        DemoEntity de = create(valueCount);
        SimpleDateFormat df = new SimpleDateFormat("MM-dd-yyyy");
        try {
            Date dt = df.parse(date);
            de.setDateTime(dt);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return de;
    }

    public static List<DemoEntity> createEntities(int count) {
        List<DemoEntity> entities = new ArrayList<>();
        for (int ii = 0; ii < count; ii++) {
            DemoEntity de = create(ii + 1);
            entities.add(de);
        }
        return entities;
    }
}
