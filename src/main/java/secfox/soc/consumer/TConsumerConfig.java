package secfox.soc.consumer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 配置文件
 * @author wangzhijie
 *
 */
public class TConsumerConfig 
{
//	private final static Logger logger = LoggerFactory.getLogger(TConsumerConfig.class);
	
	private static volatile TConsumerConfig instance = new TConsumerConfig();
	
	private Properties prop = new Properties();
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd");
	
	private boolean isSuspendPollThread = false;
	private Object lock = new Object();
	
	/*public void show(){
		 logger.info("show load t_consumer.conf value:");
		 logger.info(KEY_BROKER_ADDR+"="+getBrokerAddr());
		 logger.info(KEY_TOPIC+"="+getTopic());
		 logger.info(KEY_ZK_ADDR+"="+getZookeeperAddr());
		 logger.info(KEY_VSERIAL_CLASS+"="+getVSerialClass());
		 logger.info(KEY_ES_CONSUMER_GROUP+"="+getEsConsumerGroup());
		 logger.info(KEY_WORKERS+"="+getEsConsumerGroup());
		 
		 logger.info("show end...");
	}*/
	
	public void doSuspend(){
		if(isSuspendPollThread == true)
			return;
		synchronized (lock) {
			isSuspendPollThread = true;
		}
	}
	public void doRunning(){
		if(isSuspendPollThread == false)
			return;
		synchronized (lock) {
			isSuspendPollThread = false;
		}
	}
	
	public boolean getSuspendSignal(){
		return isSuspendPollThread;
	}
	
	public String GetPropertyFilePath(String fname) throws IOException
    {
        String jarWholePath = this.getClass().getProtectionDomain().getCodeSource().getLocation().getFile();
        jarWholePath = java.net.URLDecoder.decode(jarWholePath, "UTF-8");
        String jarPath = new File(jarWholePath).getParentFile().getAbsolutePath()+"/"+fname;
        return jarPath;
    }
	
	public static void main(String[] args) {
		System.out.println(getInstance().getEsIndex());
	}
	
	private TConsumerConfig(){
        try{
            InputStream in = new FileInputStream(new File(GetPropertyFilePath("t_consumer.conf")));
            prop.load(in);
            //启用动态生成index策略
            if(isEnableDynimicIdx()){
            	prop.setProperty(KEY_IDX, generateEsIdx(prop.getProperty(KEY_IDX)));
//            	logger.info("[ enable dynimic index policy : "+ prop.getProperty(KEY_TOPIC) +"]");
            }else{
//            	logger.info("[ disable dynimic index policy : "+ prop.getProperty(KEY_TOPIC) +"]");
            }
            in.close();
        }
        catch(Exception e){
        	e.printStackTrace();
//        	logger.error("",e);
        }
	}
	/**
	 * 动态生成es index,规则：idx-yyyy.MM.dd
	 * @param topic
	 * @return
	 */
	public String generateEsIdx(String index){
		StringBuilder idx = new StringBuilder(index);
		idx.append("-");
		idx.append(sdf.format(new Date()));
		return idx.toString();
	}
	
	public void refreshIndex(String newIdx){
		prop.setProperty(KEY_IDX, newIdx);
	}
	
	public static TConsumerConfig getInstance(){
		return instance;
	}
	
	public String getBrokerAddr(){
		if(prop!=null)
			return prop.getProperty(KEY_BROKER_ADDR);
		return "10.95.32.23:9092";
	}
	public String getTopic(){
		if(prop!=null)
			return prop.getProperty(KEY_TOPIC);
		return "push2";
	}
	public String getZookeeperAddr(){
		if(prop!=null)
			return prop.getProperty(KEY_ZK_ADDR);
		return "10.95.32.23:2181/kafka";
	}
	public String getVSerialClass(){
		if(prop!=null)
			return prop.getProperty(KEY_VSERIAL_CLASS);
		return "com.kafka.perm.producer.serial.SerialWithProstuf4Kfk";
	}
	public String getEsConsumerGroup(){
		if(prop!=null)
			return prop.getProperty(KEY_ES_CONSUMER_GROUP);
		return "es-consumer-group";
	}
	/*public int getWorkers(){
		if(prop!=null)
			return Integer.parseInt(prop.getProperty(KEY_WORKERS));
		return 3;
	}*/
	public String getEsIndex(){
		if(prop!=null)
			return prop.getProperty(KEY_IDX);
		return "data_1";
	}
	public String getEsType(){
		if(prop!=null)
			return prop.getProperty(KEY_ES_TYPE);
		return "sim";
	}
	public String getClusterName(){
		if(prop!=null)
			return prop.getProperty(KEY_CLUSTER_NAME);
		return "es";
	}
	public String getEsHostname(){
		if(prop!=null)
			return prop.getProperty(KEY_ES_HOSTNAME);
		return "127.0.0.1";
	}
	public int getEsTransPort(){
		if(prop!=null)
			return Integer.parseInt(prop.getProperty(KEY_ES_TRANS_PORT));
		return 9300;
	}
	public String getAutoCommit(){
		if(prop!=null)
			return prop.getProperty(KEY_ENABLE_AUTO_COMMIT);
		return "true";
	}
	public String getAutoCommitInterval(){
		if(prop!=null)
			return prop.getProperty(KEY_AUTO_COMMIT_INTERVAL_MS);
		return "1000";
	}
	public String getSessionTimeout(){
		if(prop!=null)
			return prop.getProperty(KEY_SESSION_TIMEOUT_MS);
		return "30000";
	}
	public String getAutoOffsetReset(){
		if(prop!=null)
			return prop.getProperty(KEY_AUTO_OFFSET_RESET);
		return "earliest";
	}
	
	public boolean isEnableDynimicIdx(){
		if(prop!=null)
			return Boolean.valueOf(prop.getProperty(KEY_ENABLE_DYNIMIC_IDX));
		return true;
	}
	
	public int getCoreSize(){
		if(prop!=null)
			return Integer.parseInt(prop.getProperty(KEY_CORE_WORKER_IDX));
		return 16;
	}
	
	public int getMaxSize(){
		if(prop!=null)
			return Integer.parseInt(prop.getProperty(KEY_MAX_WORKER_IDX));
		return 32;
	}
	
	public int getMaxQueueSize(){
		if(prop!=null)
			return Integer.parseInt(prop.getProperty(KEY_MAX_QUEUE_SIZE));
		return 200;
	}
	public int getESInsertSwitch(){
		if(prop!=null)
			return Integer.parseInt(prop.getProperty(KEY_ES_INSERT_SWITCH));
		return 1;
	}
	
	public int getScanStarTimeMinute(){
		if(prop!=null)
			return Integer.parseInt(prop.getProperty(KEY_SCAN_STARTIME_MINUTE));
		return 50;
	}
	
	public int getScanTimePeriodMinute(){
		if(prop!=null)
			return Integer.parseInt(prop.getProperty(KEY_SCAN_TIME_PERIOD_MINUTE));
		return 50;
	}
	
	
	private static final String KEY_BROKER_ADDR="broker_addr";
	private static final String KEY_TOPIC="topic";
	private static final String KEY_ZK_ADDR="zk_addr";
	private static final String KEY_VSERIAL_CLASS="vserial_class";
	private static final String KEY_ES_CONSUMER_GROUP="group.id";
//	private static final String KEY_WORKERS="workers";
	private static final String KEY_IDX="es_idx";
	private static final String KEY_ES_TYPE="es_type";
	private static final String KEY_CLUSTER_NAME="cluster.name";
	private static final String KEY_ES_HOSTNAME="es_hostname";
	private static final String KEY_ES_TRANS_PORT="es_trans_port";
	private static final String KEY_ENABLE_AUTO_COMMIT="enable.auto.commit";
	private static final String KEY_AUTO_COMMIT_INTERVAL_MS="auto.commit.interval.ms";
	private static final String KEY_SESSION_TIMEOUT_MS="session.timeout.ms";
	private static final String KEY_AUTO_OFFSET_RESET="auto.offset.reset";
	private static final String KEY_ENABLE_DYNIMIC_IDX="enable_dynimic_idx";
	private static final String KEY_CORE_WORKER_IDX="core.worker.size";
	private static final String KEY_MAX_WORKER_IDX="max.worker.size";
	private static final String KEY_MAX_QUEUE_SIZE="max_queue_size";
	private static final String KEY_ES_INSERT_SWITCH="es.insert.switch";
	private static final String KEY_SCAN_STARTIME_MINUTE="scan.startime.minute";
	private static final String KEY_SCAN_TIME_PERIOD_MINUTE="scan.time.period.minute";
	
	
}
