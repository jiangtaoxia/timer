package com.cent.timer;

import com.cent.timer.task.Task;
import com.cent.timer.trigger.CronTrigger;
import com.cent.timer.trigger.OnceTrigger;
import com.cent.timer.trigger.PeriodTrigger;
import it.sauronsoftware.cron4j.SchedulingPattern;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;

public interface Trigger {

    TriggerType type();

    void parse(String s);

    String serialize();

    void cancel();

    default String s() {
        return String.format("%s@%s", type().name(), serialize());
    }

    boolean schedule(ScheduledExecutorService scheduler, ExecutorService executor, Predicate<Task> taskGrabber, Task task);


    static Trigger build(String s) {
        String[] parts = s.split("@", 2);
        String type = parts[0];
        Trigger trigger = null;
        switch (TriggerType.valueOf(type)) {
            case ONCE:
                trigger = new OnceTrigger();
                break;
            case PERIOD:
                trigger = new PeriodTrigger();
                break;
            case CRON:
                trigger = new CronTrigger();
                break;
        }
        trigger.parse(parts[1]);
        return trigger;
    }

    static OnceTrigger once(Date startTime) {
        return new OnceTrigger(startTime);
    }

    /**
     * 不同进程调用这个函数得到的起始时间是不一样的，所以这个函数一般仅用于测试，它会导致任务在不同的进程中分配不均匀
     */
    static OnceTrigger onceOfDelay(int delay) {
        Date now = new Date();
        Calendar cal = Calendar.getInstance();
        cal.setTime(now);
        cal.add(Calendar.SECOND, delay);
        return once(cal.getTime());
    }

    static PeriodTrigger period(Date startTime, Date endTime, int period) {
        return new PeriodTrigger(startTime, endTime, period);
    }

    static PeriodTrigger period(Date startTime, int period) {
        return period(startTime, new Date(Long.MAX_VALUE), period);
    }

    /**
     * 不同进程调用这个函数得到的起始时间是不一样的，所以这个函数一般仅用于测试，它会导致任务在不同的进程中分配不均匀
     *
     * @param delay
     * @param period
     * @return
     */
    static PeriodTrigger periodOfDelay(int delay, int period) {
        Date now = new Date();
        Calendar cal = Calendar.getInstance();
        cal.setTime(now);
        cal.add(Calendar.SECOND, delay);
        return new PeriodTrigger(cal.getTime(), new Date(Long.MAX_VALUE), period);
    }

    static CronTrigger cron(String expr) {
        if (!SchedulingPattern.validate(expr)) {
            throw new IllegalArgumentException("cron expression illegal");
        }
        return new CronTrigger(expr);
    }

    static CronTrigger cronOfMinutes(int minutes) {
        return cron(String.format("*/%d * * * *", minutes));
    }

    static CronTrigger cronOfHours(int hours, int minuteOffset) {
        return cron(String.format("%d */%d * * *", minuteOffset, hours));
    }

    static CronTrigger cronOfDays(int days, int hourOffset, int minuteOffset) {
        return cron(String.format("%d %d */%d * *", minuteOffset, hourOffset, days));
    }
}
