package com.cent.timer;

import java.util.Map;

public interface ITaskStore {

    /**
     * 存储器中任务列表版本号
     */
    long getRemoteVersion();

    /**
     * 获取所有的任务触发器
     */
    Map<String, String> getAllTriggers();

    /**
     * 保存所有的任务触发器
     */
    void saveAllTriggers(long version, Map<String, String> triggers);

    /**
     * 抢占任务（是否可以抢到任务）
     */
    boolean grabTask(String name);

}
