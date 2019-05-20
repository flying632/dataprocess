package com.neu.dataprocess.service;

import com.neu.dataprocess.entity.Host;

import javax.mail.MessagingException;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @author fengyuluo
 * @createtime 10:17 2019/4/23
 */
public interface ElasticsearchService {
    //获取所有主机的名称
    ArrayList<Host> getAllHosts() throws IOException;

    //检查Disk的使用量
    void diskCheck() throws IOException, MessagingException;

    //检查cpu负载
    void cpuCheck() throws IOException, MessagingException;

    //检查内存是否足够
    void memoryCheck() throws IOException, MessagingException;

    //检查可用性
    void availableCheck() throws IOException, MessagingException;

    //检查所有的检查项
    void checkAll() throws  IOException,MessagingException;

}
