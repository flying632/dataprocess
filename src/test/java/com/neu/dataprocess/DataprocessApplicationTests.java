package com.neu.dataprocess;

import com.neu.dataprocess.service.ElasticsearchService;
import com.neu.dataprocess.service.impl.ElasticsearchServiceImpl;
import com.neu.dataprocess.task.QuartzJdbc;
import com.neu.dataprocess.util.ClientUtil;
import com.neu.dataprocess.util.Consumer;
import com.neu.dataprocess.util.MailUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.mail.MessagingException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DataprocessApplicationTests {

    @Test
    public void ESClient() throws IOException, MessagingException {
        ElasticsearchService service = new ElasticsearchServiceImpl();
        service.getAllHosts();
        service.diskCheck();
    }
    @Test
    public void testAvailable() throws IOException, MessagingException {
        ElasticsearchService service = new ElasticsearchServiceImpl();
        service.getAllHosts();
        service.availableCheck();
    }

    @Test
    public void mailTest() throws UnsupportedEncodingException, MessagingException {
        MailUtil.sendMail("bs_632@163.com","test","test");
    }

    @Test
    public void schedulerTest() {
        QuartzJdbc.startSchedule();
    }
    @Test
    public void testConsumer() {
        Consumer consumer = new Consumer();
        consumer.consumeKafka();
    }

}
