package com.cent.timer;

import cn.hutool.core.lang.Console;
import cn.hutool.log.StaticLog;
import com.cent.timer.task.Task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class DistributedScheduler {

    // 任务触发器存储器，持久化
    private ITaskStore store;

    // 任务列表版本号（任务重加载）
    private long version;

    /**
     * 任务变更时，必须递增版本号，才能触发其它进程的任务重新加载
     */
    public DistributedScheduler version(int version) {
        if (version < 0) {
            throw new IllegalArgumentException("tasks version must be non-negative!");
        }
        this.version = version;
        return this;
    }

    // 所有的任务
    private Map<String, Task> allTasks = new ConcurrentHashMap<>();

    // 任务触发器
    private Map<String, Trigger> triggers = new ConcurrentHashMap<>();

    // 任务调度器（调度线程）
    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    // 任务运行器（运行线程）
    private ExecutorService executor;

    // 任务完成监听器
    private List<ISchedulerListener> listeners = new CopyOnWriteArrayList<>();

    // 待重加载的触发器
    private Map<String, Trigger> reloadingTriggers = new HashMap<>();


    public DistributedScheduler(ITaskStore store) {
        this(store, Runtime.getRuntime().availableProcessors() * 2);
    }

    /**
     * @param store   任务存储器
     * @param threads 任务运行线程数（默认 cores*2）
     */
    public DistributedScheduler(ITaskStore store, int threads) {
        this.store = store;
        this.executor = Executors.newFixedThreadPool(threads);
    }

    /**
     * 注册调度监听器
     *
     * @param listener
     * @return
     */
    public DistributedScheduler listener(ISchedulerListener listener) {
        this.listeners.add(listener);
        return this;
    }

    /**
     * 注册任务
     *
     * @param trigger
     * @param task
     * @return
     */
    public DistributedScheduler register(Trigger trigger, Task task) {
        if (this.triggers.containsKey(task.name())) {
            throw new IllegalArgumentException("task name duplicated!");
        }
        this.triggers.put(task.name(), trigger);
        this.allTasks.put(task.name(), task);
        task.callback(ctx -> {
            for (ISchedulerListener listener : listeners) {
                try {
                    listener.onComplete(ctx);
                } catch (Exception ex) {
//                    LOG.error("invoke task {} complete listener error", ctx.task().name(), ex);
                }
            }
        });
        return this;
    }

    /**
     * 手动触发任务运行
     */
    public void triggerTask(String name) {
        Task task = this.allTasks.get(name);
        if (task != null) {
            task.run();
        }
    }

    /**
     * 启动调度器
     */
    public void start() {
        // 先保存触发器（如果任务变更，会触发其它进程重加载）
        this.saveTriggers();
        // 调度任务
        this.scheduleTasks();
        // 监控任务版本（重加载）
        this.scheduleReload();
        // 监听回调
        listeners.forEach(listener -> {
            try {
                listener.onStartup();
            } catch (Exception e) {
                System.out.println(e);
            }
        });
    }

    /**
     * 每秒校验是否需要重新加载
     */
    private void scheduleReload() {
        // 1s 对比一次
        this.scheduler.scheduleWithFixedDelay(() -> {
            try {
                if (this.reloadIfChanged()) {
                    this.reScheduleTasks();
                }
            } catch (Exception e) {
//                LOG.error("reloading tasks error", e);
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    private void reScheduleTasks() {
        this.reloadingTriggers.forEach((name, trigger) -> {
            Task task = this.allTasks.get(name);
            if (trigger == null) {
                // deleting
//                LOG.warn("unscheduling task {}", name);
                triggers.get(name).cancel();
                triggers.remove(name);
            } else {
                Trigger oldTrigger = triggers.get(name);
                if (oldTrigger != null) {
                    // updating, cancel the old first
//                    LOG.warn("unscheduling task {}", name);
                    oldTrigger.cancel();
                }
                triggers.put(name, trigger);
                // new
//                LOG.warn("scheduling task {}", name);
                trigger.schedule(scheduler, executor, this::grabTaskSilently, task);
            }
        });
        this.reloadingTriggers.clear();
        // 回调
        for (ISchedulerListener listener : listeners) {
            try {
                listener.onReload();
            } catch (Exception e) {
//                LOG.error("invoke scheduler reload listener error", e);
            }
        }
    }

    private boolean reloadIfChanged() {
        long remoteVersion = store.getRemoteVersion();
        if (remoteVersion > version) {
            this.version = remoteVersion;
            this.reload();
            return true;
        }
        return false;
    }

    private void reload() {
        Map<String, String> raws = store.getAllTriggers();
        Map<String, Trigger> reloadings = new HashMap<>();
        raws.forEach((name, raw) -> {
            // 内存里必须有这个任务（新增任务，老版本的进程里就没有）
            if (this.allTasks.containsKey(name)) {
                Trigger trigger = Trigger.build(raw);
                Trigger oldTrigger = this.triggers.get(name);
                if (oldTrigger == null || !oldTrigger.equals(trigger)) {
                    // new or changed
                    reloadings.put(name, trigger);
                }
            }
        });
        // deleted
        this.triggers.forEach((name, trigger) -> {
            if (!raws.containsKey(name)) {
                reloadings.put(name, null);
            }
        });
        this.reloadingTriggers = reloadings;
    }

    private void scheduleTasks() {
        this.triggers.forEach((name, trigger) -> {
            Task task = allTasks.get(name);
            if (task == null) {
                return;
            }
            trigger.schedule(scheduler, executor, this::grabTaskSilently, task);
        });
    }

    private void saveTriggers() {
        Map<String, String> triggersRaw = new HashMap<>();
        this.triggers.forEach((name, trigger) -> triggersRaw.put(name, trigger.s()));
        this.store.saveAllTriggers(version, triggersRaw);
    }

    private boolean grabTaskSilently(Task task) {
        if (task.isConcurrent()) {
            return true;
        }
        try {
            return store.grabTask(task.name());
        } catch (Exception e) {
            return false;
        }
    }

    public void stop() {
        this.cancelAllTasks();
        this.scheduler.shutdown();
        try {
            this.scheduler.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
        this.executor.shutdown();
        try {
            if (!this.executor.awaitTermination(10, TimeUnit.SECONDS)) {
                StaticLog.warn("work is not complete while stopping scheduler");
            }
        } catch (InterruptedException e) {
            StaticLog.error("work is not complete while stopping scheduler", e);
        }
        // 回调
        for (ISchedulerListener listener : listeners) {
            try {
                listener.onStop();
            } catch (Exception e) {
                StaticLog.error("invoke scheduler stop listener error", e);
            }
        }
    }

    private void cancelAllTasks() {
        this.triggers.forEach((name, trigger) -> {
            StaticLog.warn("cancelling task {}", name);
            trigger.cancel();
        });
        this.triggers.clear();
    }
}
