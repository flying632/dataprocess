package com.neu.dataprocess.service;

import com.neu.dataprocess.entity.Host;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author fengyuluo
 * @createTime 10:17 2019/4/23
 */
public interface ElasticsearchService {
    //获取所有主机的名称
    public ArrayList<Host> getAllHosts() throws IOException;

    //检查Disk的使用量
    public boolean diskIOCheck();

    //检查内存占用
    public boolean memoryCheck();

    //检查可用性
    public boolean avaliableCheck();

}
