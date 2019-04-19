package com.neu.dataprocess;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DataprocessApplicationTests {

    @Test
    public void contextLoads() {
        Consumer consumer = new Consumer();
        consumer.consumeKafka();
    }


}
