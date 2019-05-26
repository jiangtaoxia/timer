package com.cent.timer;

import cn.hutool.core.lang.Console;
import cn.hutool.log.StaticLog;
import com.cent.timer.store.MemoryTaskStore;
import com.cent.timer.task.Task;

import java.util.Date;

public class Test {

    public static void main(String[] args) {
        ITaskStore store = new MemoryTaskStore();
        DistributedScheduler scheduler = new DistributedScheduler(store, 5);
        scheduler.register(Trigger.once(new Date()), Task.of("once1", () -> {
            System.out.println("once1");
        }));
        Console.log("==============");
        StaticLog.info("This is static {} log.", "INFO");
        StaticLog.info("This is static {} log.", "INFO");
        StaticLog.info("This is static {} log.", "INFO");
        StaticLog.info("This is static {} log.", "INFO");
        StaticLog.info("This is static {} log.", "INFO");


//        scheduler.register(Trigger.period(new Date(0), 1), Task.of("period2", () -> {
//            System.out.println("period2 is done");
//        }));
//        scheduler.register(Trigger.cronOfMinutes(2), Task.of("cron3", () -> {
//            System.out.println("cron3");
//        }));
//        scheduler.register(Trigger.period(new Date(0), 10), Task.of("period4", () -> {
//            System.out.println("period4");
//        }));
        scheduler.version(4);
        scheduler.listener(ctx -> {
            System.out.println(ctx.task().name() + " is complete");
        });
        scheduler.start();
    }
}
