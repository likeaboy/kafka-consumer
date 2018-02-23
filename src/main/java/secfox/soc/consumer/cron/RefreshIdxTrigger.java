package secfox.soc.consumer.cron;

import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import secfox.soc.consumer.TConsumerConfig;
/**
 * 每天凌晨零点零分生成新的index
 * @author wangzhijie
 *
 */
@Deprecated
public class RefreshIdxTrigger extends AbstractCronComponent{

	private final static Logger logger = LoggerFactory.getLogger(RefreshIdxTrigger.class);
	private static volatile RefreshIdxTrigger instance = new RefreshIdxTrigger();
	private static final long PERIOD_DAY = 24 * 60 * 60 * 1000;

	private RefreshIdxTrigger() {
	}

	public static RefreshIdxTrigger getInstance() {
		return instance;
	}

	public void start() {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		Date execDate = calendar.getTime();
		// execDate 比当前时间小，则取下一天的凌晨
		if (execDate.before(new Date())) {
			execDate = addDay(execDate, 1);
		}

		Timer timer = new Timer();
		RefreshIdxTask refreshIndex = new RefreshIdxTask();
		timer.schedule(refreshIndex, execDate, PERIOD_DAY);
		logger.info("refresh index trigger start...");
	}

	class RefreshIdxTask extends TimerTask {

		@Override
		public void run() {
			// 刷新es index
			TConsumerConfig tconf = TConsumerConfig.getInstance();
			if(!tconf.getEsIndex().equals("")){
				//skyeye-las_event-2017.11.08-2017.11.09
				String newIdx = tconf.generateEsIdx(tconf.getEsIndex().substring(0,tconf.getEsIndex().length()-11));
				tconf.refreshIndex(newIdx);
				logger.info("refresh index,new idx=" + newIdx);
			}else{
				logger.info("refresh index error, index is null");
			}
		}

	}
}
