package com.neu.dataprocess.config;
 
import com.neu.dataprocess.task.WarnJob;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;
import org.springframework.beans.factory.annotation.Autowire;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.quartz.CronTriggerFactoryBean;
import org.springframework.scheduling.quartz.JobDetailFactoryBean;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.scheduling.quartz.SpringBeanJobFactory;

import javax.sql.DataSource;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author  taos
 */
@Configuration
@EnableScheduling
public class QuartzConfiguration {
    /**
     * 继承org.springframework.scheduling.quartz.SpringBeanJobFactory
     * 实现任务实例化方式
     */
    public static class AutowiringSpringBeanJobFactory extends SpringBeanJobFactory implements
            ApplicationContextAware {

        private transient AutowireCapableBeanFactory beanFactory;

        @Override
        public void setApplicationContext(final ApplicationContext context) {
            beanFactory = context.getAutowireCapableBeanFactory();
        }

        /**
         * 将job实例交给spring ioc托管
         * 我们在job实例实现类内可以直接使用spring注入的调用被spring ioc管理的实例
         *
         * @param bundle
         * @return
         * @throws Exception
         */
        @Override
        protected Object createJobInstance(final TriggerFiredBundle bundle) throws Exception {
            final Object job = super.createJobInstance(bundle);
            /**
             * 将job实例交付给spring ioc
             */
            beanFactory.autowireBean(job);
            return job;
        }
    }

    private static final String QUARTZ_JOB_GROUP = "QuartzJobGroup";

    private static final String JOB_ = "_job";
    private static final String TRIGGER_ = "_trigger";

    /**
     * 注入任务调度器
     */
    @Qualifier("schedulers")
    @Autowired
    private Scheduler scheduler;

    /**
     * 配置任务工厂实例
     *
     * @param applicationContext spring上下文实例
     * @return
     */
    @Bean
    public JobFactory jobFactory(ApplicationContext applicationContext) {
        /**
         * 采用自定义任务工厂 整合spring实例来完成构建任务
         * see {@link AutowiringSpringBeanJobFactory}
         */
        AutowiringSpringBeanJobFactory jobFactory = new AutowiringSpringBeanJobFactory();
        jobFactory.setApplicationContext(applicationContext);
        return jobFactory;
    }

    /**
     * 配置任务调度器
     * 使用项目数据源作为quartz数据源
     *
     * @param jobFactory 自定义配置任务工厂
     * @param dataSource 数据源实例
     * @return
     * @throws Exception
     */
    @Bean(name = "schedulers", destroyMethod = "destroy", autowire = Autowire.NO)
    public SchedulerFactoryBean schedulerFactoryBean(JobFactory jobFactory, DataSource dataSource, @Qualifier("triggers") CronTriggerImpl[] triggers) throws Exception {
        SchedulerFactoryBean schedulerFactoryBean = new SchedulerFactoryBean();
        //将spring管理job自定义工厂交由调度器维护
        schedulerFactoryBean.setJobFactory(jobFactory);
        //设置覆盖已存在的任务
        schedulerFactoryBean.setOverwriteExistingJobs(true);
        //项目启动完成后，等待2秒后开始执行调度器初始化
        schedulerFactoryBean.setStartupDelay(2);
        //设置调度器自动运行
        schedulerFactoryBean.setAutoStartup(true);
        //设置数据源，使用与项目统一数据源
        schedulerFactoryBean.setDataSource(dataSource);
        //设置上下文spring bean name
        schedulerFactoryBean.setApplicationContextSchedulerContextKey("applicationContext");
        //设置配置文件位置
        schedulerFactoryBean.setConfigLocation(new ClassPathResource("/quartz.properties"));
        //设置定时任务
        schedulerFactoryBean.setTriggers(createTriggers());// 直接使用配置文件
        return schedulerFactoryBean;
    }




    /**
     * @auth: taos
     * 创建job Detail 的公共方法
     * @param jobClass
     * @return
     */
    private JobDetail create(Class<? extends Job> jobClass) {
        JobDetailFactoryBean d = new JobDetailFactoryBean();
        d.setDurability(true);
        d.setRequestsRecovery(true);
        d.setJobClass(jobClass);
        d.setName(jobClass.getSimpleName() + JOB_);
        d.setGroup(jobClass.getSimpleName() + QUARTZ_JOB_GROUP);
        d.afterPropertiesSet();
        JobDetail jd= d.getObject();
        //jd.getJobDataMap().put("key", 123);//如果想通过jobDataMap传递值，在这里添加
        return jd;
    }

    /**
     * @auth: taos
     * 创建Trigger 工具方法
     * @param t
     * @param cronExpression
     * @return
     * @throws ParseException
     */
    private CronTriggerImpl createTrigger(Class<? extends Job> t, String cronExpression) throws ParseException {
        CronTriggerFactoryBean c = new CronTriggerFactoryBean();
        c.setJobDetail(create(t));
        c.setCronExpression(cronExpression);
        c.setName(t.getSimpleName() + TRIGGER_);
        c.setGroup(t.getSimpleName() + QUARTZ_JOB_GROUP);
        c.afterPropertiesSet();
        return (CronTriggerImpl)c.getObject();
    }
//==============================================配置你的定时任务==============================================================
    @Bean(name = "triggers")
    public CronTriggerImpl[] createTriggers()  throws ParseException
    {
        List<CronTriggerImpl> triggers = new ArrayList<CronTriggerImpl>();
        //配置任务，以及执行时间 按你的需要添加多个任务：任务所在类.class   cron表达式
        triggers.add(createTrigger(WarnJob.class,"0/10 * * * * ?"));
//        triggers.add(createTrigger(OrderTask.class, "0/20 * * * * ?"));
//
//        triggers.add(createTrigger(EmActivityTask.class, "0/10 * * * * ?"));

        return triggers.toArray(new CronTriggerImpl[triggers.size()]);
    }

}