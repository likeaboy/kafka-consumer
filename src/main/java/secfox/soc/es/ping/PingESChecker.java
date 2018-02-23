package secfox.soc.es.ping;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import secfox.soc.consumer.TConsumerConfig;

public class PingESChecker extends TimerTask{
	
	private final static Logger logger = LoggerFactory.getLogger(PingESChecker.class);
	
	public static final int SCHEDULE_CYCLE = 30 * 1000;
	
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		logger.info("[PingESChecker schedule run , date='"+sdf.format(new Date())+"', timestamp="+System.currentTimeMillis()+"]");
		boolean pingSuccess = PingIndexManager.getInstance().ping();
		if(pingSuccess) {
			TConsumerConfig.getInstance().doRunning();
			logger.info("[ping success, do running...]");
			logger.info("[poll thread status(isSuspendSignal) is : "+ TConsumerConfig.getInstance().getSuspendSignal()+"]");
		}else{
			logger.info("[ping fail, do noting...]");
		}
	}

}
