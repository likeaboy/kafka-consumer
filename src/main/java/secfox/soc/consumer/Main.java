package secfox.soc.consumer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Timer;

import kafka.log.LogCleaner;

import org.apache.log4j.PropertyConfigurator;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import secfox.soc.consumer.cron.RefreshIdxTrigger;
import secfox.soc.consumer.cron.SentryMaster;
import secfox.soc.consumer.mutil.PollThreadHandler;
import secfox.soc.es.ping.PingESChecker;
import secfox.soc.es.ping.PingIndexManager;
/**
 * 启动类
 * @author wangzhijie
 *
 */
public class Main {

	private final static Logger logger = LoggerFactory.getLogger(Main.class);
	
	static {   
		try {
			PropertyConfigurator.configure(TConsumerConfig.getInstance().GetPropertyFilePath("log4j.properties"));
			logger.info("load log4j from : " + TConsumerConfig.getInstance().GetPropertyFilePath("log4j.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}   
    }  
	
	private static void createPollThreadHandler(TransportClient client){
		int i = 1;
		logger.info("consumer " + i +" start...");
		Thread t = new Thread(new PollThreadHandler(client));
		t.setName("PollThread-"+i);
		t.start();
	}
	
	private static TransportClient initTransportClient(){
		TransportClient client = null;
		TConsumerConfig tconf = TConsumerConfig.getInstance();
		Settings settings = Settings.builder()
		        .put("cluster.name", tconf.getClusterName()).build();
		try {
			 client = new PreBuiltTransportClient(settings)
			.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(tconf.getEsHostname()), tconf.getEsTransPort()));
		} catch (UnknownHostException e) {
			logger.error("",e);
		}
		
		if(client == null)
			logger.info("[init TransportClient client, client is null]");
		else
			logger.info("[init TransportClient client, client is "+client+"]");
		
		return client;
	}
	
	public static void main(String[] args) {
		logger.info("main start...");
		TransportClient client = initTransportClient();
		createPollThreadHandler(client);
		PingIndexManager.getInstance().initPingIndex(client);
		RefreshIdxTrigger.getInstance().start();
		/*SentryMaster.getInstance().start();*/
		
		//启动后台线程30s定时ping es
		Timer timer = new Timer(); 
		timer.schedule(new PingESChecker(), PingESChecker.SCHEDULE_CYCLE, PingESChecker.SCHEDULE_CYCLE); 
		
		//启用动态生成index策略
        if(TConsumerConfig.getInstance().isEnableDynimicIdx()){
        	logger.info("[ enable dynimic index policy : "+ TConsumerConfig.getInstance().getEsIndex() +"]");
        }else{
        	logger.info("[ disable dynimic index policy : "+ TConsumerConfig.getInstance().getEsIndex() +"]");
        }
	}
}
