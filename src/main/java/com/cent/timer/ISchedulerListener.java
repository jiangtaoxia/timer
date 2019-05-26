package com.cent.timer;

import com.cent.timer.task.TaskContext;

public interface ISchedulerListener {

    void onComplete(TaskContext ctx);

    default void onStartup() {
    }

    default void onReload() {
    }

    default void onStop() {
    }

}
