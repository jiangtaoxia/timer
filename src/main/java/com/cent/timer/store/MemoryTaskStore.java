package com.cent.timer.store;

import com.cent.timer.ITaskStore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryTaskStore implements ITaskStore {

    private long version;

    private Map<String, String> triggers = new ConcurrentHashMap<>();

    @Override
    public long getRemoteVersion() {
        return version;
    }

    @Override
    public Map<String, String> getAllTriggers() {
        return triggers;
    }

    @Override
    public void saveAllTriggers(long version, Map<String, String> triggers) {
        this.triggers = triggers;
        this.version = version;
    }

    @Override
    public boolean grabTask(String name) {
        return true;
    }

}
