/*package secfox.soc.consumer.cron;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LogClearer extends AbstractCronComponent{
	private final static Logger logger = LoggerFactory.getLogger(LogClearer.class);
	private static volatile LogClearer instance = new LogClearer();
	private static final long PERIOD_DAY = 24 * 60 * 60 * 1000;
	private SimpleDateFormat sdf = new SimpleDateFormat("'.'yyyy-MM-dd");
	
	private static final String PATH = "/app/kafka-consumer/logs/";
	private static final String FILENAME="consumer.log";
	
	 *//**
	   * 最近保留天数
	   *//*
	  private int  maxBackupIndex  = 7;

	private LogClearer() {
	}

	public static LogClearer getInstance() {
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
		LogClearerTask logClearTask = new LogClearerTask();
		timer.schedule(logClearTask, execDate, PERIOD_DAY);
		logger.info("log clear trigger start...");
	}

	class LogClearerTask extends TimerTask {

		@Override
		public void run() {
			logger.info("[log clear task running,check which log should be deleted...]");
			File file = new File(PATH+FILENAME);
			//获取日志文件列表，控制数量，实现清理策略
	        if (file.getParentFile().exists()){
	            File[] files = file.getParentFile().listFiles();
	            Long[] dateArray = new Long[files.length];
	            for (int i = 0; i < files.length; i++) {
	                File fileItem = files[i];
	                String fileDateStr = fileItem.getName().replace(file.getName(), "");
	                Date filedate = null;
	                try {
	                    filedate = sdf.parse(fileDateStr);
	                    long fileDateLong = filedate.getTime();
	                    dateArray[i] = fileDateLong;
	                } catch (ParseException e) {
	                	logger.error("Parse File Date Throw Exception : " + e.getMessage());
	                }
	            }
	            Arrays.sort(dateArray);
	            if (dateArray.length > maxBackupIndex) {
	                for (int i = 0; i < dateArray.length - maxBackupIndex; i++) {
	                    String dateFileName = file.getPath() + sdf.format(dateArray[i]);
	                    File dateFile = new File(dateFileName);
	                    if (dateFile.exists()) {
	                    	logger.info("[delete log file : "+dateFile.getName()+"]");
	                        dateFile.delete();
	                    }
	                }
	            }
	        }
			
		}

	}
}
*/