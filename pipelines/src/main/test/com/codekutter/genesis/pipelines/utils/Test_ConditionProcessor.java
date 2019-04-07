package com.codekutter.genesis.pipelines.utils;

import com.codekutter.zconfig.common.LogUtils;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class Test_ConditionProcessor {

    @Test
    void matches() {
        try {
            Car car = CarFactory.createCar(1000);
            String query = String.format(
                    "(manufacturer = 'Ford' OR manufacturer = '%s') " +
                            "AND price <= %f " +
                            "AND color NOT IN ('WHITE', 'GREEN')", car.manufacturer,
                    car.price + 1000);
            ConditionProcessor<Car> processor = ConditionProcessorFactory.getProcessor(Car.class);
            assertTrue(processor.matches(car, query));
            query = String.format(
                    "(manufacturer = 'Ford' OR manufacturer = '%s') " +
                            "AND price <= %f " +
                            "AND color NOT IN ('WHITE', 'GREEN')", car.manufacturer,
                    car.price - 1000);
            assertFalse(processor.matches(car, query));
        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            fail(ex.getLocalizedMessage());
        }
    }

    @Test
    void filter() {
        try {
            Set<Car> cars = CarFactory.createCollectionOfCars(10000);
            String query = String.format(
                    "(manufacturer = 'Ford' OR manufacturer = '%s') " +
                            "AND price <= %f " +
                            "AND color NOT IN ('WHITE', 'GREEN')", "Honda",
                    10000.00);
            ConditionProcessor<Car> processor = ConditionProcessorFactory.getProcessor(Car.class);
            assertFalse(processor.filter(cars, query).isEmpty());

        } catch (Exception ex) {
            LogUtils.error(getClass(), ex);
            fail(ex.getLocalizedMessage());
        }
    }
}