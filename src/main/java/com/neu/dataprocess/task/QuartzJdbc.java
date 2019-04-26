package com.neu.dataprocess.task;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.List;
import java.util.SimpleTimeZone;

/**
 * @author fengyuluo
 * @createTime 11:34 2019/4/26
 */
public class QuartzJdbc {
    public static void startSchedule() {
        try {
            //创建一个JobDetail实例，指定Quartz
            JobDetail jobDetail = JobBuilder.newJob(WarnJob.class)
                    .withIdentity("job1_1","jGroup1")
                    .build();

            //触发器类型
            SimpleScheduleBuilder builder = SimpleScheduleBuilder
                    .repeatSecondlyForever(10);

            //创建Trigger
            Trigger trigger = TriggerBuilder.newTrigger()
                    .withIdentity("trigger1_1","tGroup1").startNow()
                    .withSchedule(builder)
                    .build();

            //创建sceduler
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
            scheduler.start();

            //调度执行
            scheduler.scheduleJob(jobDetail,trigger);

        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }
    public static void resumeJob() {
        try {
            SchedulerFactory schedulerFactory = new StdSchedulerFactory();
            Scheduler scheduler = schedulerFactory.getScheduler();
            JobKey jobKey = new JobKey("job1_1","jGroup1");
            List<? extends Trigger> triggers = scheduler.getTriggersOfJob(jobKey);
            if (triggers.size() > 0) {
                for (Trigger tg : triggers) {
                    if (tg instanceof CronTrigger || tg instanceof SimpleTrigger)
                        scheduler.resumeJob(jobKey);
                }
                scheduler.start();
            }

        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }
}
