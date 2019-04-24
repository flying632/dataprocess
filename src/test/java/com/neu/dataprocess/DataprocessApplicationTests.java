package com.neu.dataprocess;

import com.neu.dataprocess.service.ElasticsearchService;
import com.neu.dataprocess.service.impl.ElasticsearchServiceImpl;
import com.neu.dataprocess.util.ClientUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DataprocessApplicationTests {

    @Test
    public void ESClient() throws IOException {
        ElasticsearchService service = new ElasticsearchServiceImpl();
        service.getAllHosts();
    }


}
