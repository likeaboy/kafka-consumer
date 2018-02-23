package secfox.soc.consumer.cron;

import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import secfox.soc.consumer.TConsumerConfig;
import secfox.soc.consumer.filter.Sentry;

public class SentryMaster extends AbstractCronComponent{
	
	private final static Logger logger = LoggerFactory.getLogger(SentryMaster.class);
	private static volatile SentryMaster instance = new SentryMaster();
	
	//one day
	private static final long PERIOD_DAY = 24 * 60 * 60 * 1000;

	private SentryMaster() {
	}

	public static SentryMaster getInstance() {
		return instance;
	}
	
	public void start() {
		int minute = TConsumerConfig.getInstance().getScanStarTimeMinute();
		//default : 23:50:00
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, 23);
		calendar.set(Calendar.MINUTE, minute);
		calendar.set(Calendar.SECOND, 0);
		Date execDate = calendar.getTime();
		// execDate 比当前时间小，则取下一天的凌晨
		if (execDate.before(new Date())) {
			execDate = addDay(execDate, 1);
		}

		Timer timer = new Timer();
		EnableSentryCheckTask enableSentryChkTask = new EnableSentryCheckTask();
		timer.schedule(enableSentryChkTask, execDate, PERIOD_DAY);
		logger.info("sentry master start..., scan start time is : {}:{}:{}",23,minute,"00");
		Sentry.updateBoundary();
	}
	
	class EnableSentryCheckTask extends TimerTask {

		@Override
		public void run() {
			Sentry.setScanStartTime(System.currentTimeMillis());
			boolean current = Sentry.isCheck();
			Sentry.enableCheck();
			logger.info("[do Sentry.enableCheck()...,Sentry.isCheck from : {} to {}]",current,Sentry.isCheck());
			//开启计时器，delay 指定时间执行一次
			Timer timer = new Timer();
			DisableSentryCheckTask task = new DisableSentryCheckTask();
			long delay = TConsumerConfig.getInstance().getScanTimePeriodMinute() * 60 * 1000;
			timer.schedule(task, delay);
			logger.info("[start DisableSentryCheckTask , delay {} will be execute...]",delay);
		}
	}
	
	class DisableSentryCheckTask extends TimerTask {
		@Override
		public void run() {
			Sentry.setScanEndTime(System.currentTimeMillis());
			boolean current = Sentry.isCheck();
			Sentry.disableCheck();
			logger.info("[do Sentry.disableCheck...,Sentry.isCheck from : {} to {}]",current,Sentry.isCheck());
			logger.info("[finish scan...,running period {} ~ {}]",Sentry.format(Sentry.getScanStartTime()),Sentry.format(Sentry.getScanEndTime()));
			//update boundary when scan finished
			Sentry.updateBoundary();
			//update tconf es index
			Sentry.updateTconfEsIndex();
		}
	}
}
