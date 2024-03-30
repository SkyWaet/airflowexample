package com.skywaet;

import com.skywaet.airflowexample.producer.AlwaysErrorService;
import com.skywaet.airflowexample.producer.AlwaysInfoService;
import com.skywaet.airflowexample.producer.RandomService;

import java.util.concurrent.Executors;

public class Producer {
    public static void main(String[] args) {
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        executor.submit(new AlwaysErrorService());
        executor.submit(new AlwaysInfoService());
        executor.submit(new RandomService());

        while (true) {
        }
    }
}