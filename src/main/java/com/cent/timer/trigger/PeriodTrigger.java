package com.cent.timer.trigger;

import com.cent.timer.Trigger;
import com.cent.timer.TriggerType;
import com.cent.timer.task.Task;
import com.cent.timer.util.TimeFormat;

import java.util.Date;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class PeriodTrigger implements Trigger {

    // 任务最早执行时间
    private Date startTime;
    // 任务最晚执行时间
    private Date endTime;
    // 间隔多少秒
    private int period;

    private ScheduledFuture<?> future;

    public PeriodTrigger() {
    }

    public PeriodTrigger(Date startTime, Date endTime, int period) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.period = period;
    }

    public Date getStartTime() {
        return startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public int getPeriod() {
        return period;
    }

    @Override
    public TriggerType type() {
        return TriggerType.PERIOD;
    }

    @Override
    public void parse(String s) {
        TimeFormat.SimplerDateFormat formatter = TimeFormat.ISOFormatter.take();
        String[] parts = s.split("\\|");
        this.startTime = formatter.parseString(parts[0]);
        this.endTime = formatter.parseString(parts[1]);
        this.period = Integer.parseInt(parts[2]);
    }

    @Override
    public String serialize() {
        TimeFormat.SimplerDateFormat formatter = TimeFormat.ISOFormatter.take();
        StringJoiner sj = new StringJoiner("|");
        sj.add(formatter.format(this.startTime));
        sj.add(formatter.format(this.endTime));
        sj.add(String.valueOf(this.period));
        return sj.toString();
    }

    @Override
    public boolean schedule(ScheduledExecutorService scheduler, ExecutorService executor, Predicate<Task> taskGrabber, Task task) {
        long now = System.currentTimeMillis();
        if (now >= this.getEndTime().getTime()) {
            return false;
        }
        long delay = 0;
        if (now <= this.getStartTime().getTime()) {
            delay = this.getStartTime().getTime() - now;
        } else {
            // 如果正好卡在周期点上那就立即执行
            // 否则延迟到下一个周期点
            long ellapsed = (now - this.getStartTime().getTime()) % (this.getPeriod() * 1000);
            if (ellapsed > 0) {
                delay = this.getPeriod() * 1000 - ellapsed;
            }
        }
        this.future = scheduler.scheduleAtFixedRate(() -> {
            // 到结束时间了，结束定时器
            if (System.currentTimeMillis() >= this.getEndTime().getTime()) {
                cancel();
                return;
            }
            executor.submit(() -> {
                if (taskGrabber.test(task)) {
                    task.run();
                }
            });
        }, delay, this.getPeriod() * 1000, TimeUnit.MILLISECONDS);
        return this.future != null;
    }

    @Override
    public void cancel() {
        if (this.future != null) {
            this.future.cancel(false);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof PeriodTrigger)) {
            return false;
        }
        PeriodTrigger otherTrigger = (PeriodTrigger) other;
        if (!this.startTime.equals(otherTrigger.startTime)) {
            return false;
        }
        if (!this.endTime.equals(otherTrigger.endTime)) {
            return false;
        }
        return this.period == otherTrigger.period;
    }

}
