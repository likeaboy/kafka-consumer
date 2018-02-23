package secfox.soc.consumer.mutil;

import java.util.List;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import secfox.soc.consumer.TConsumerConfig;
/**
 * backup
 * @author wangzhijie
 *
 */
public class BackupHandler implements Runnable{
	
	private final static Logger logger = LoggerFactory.getLogger(BackupHandler.class);
	
	
	public BackupHandler(){
	}
	
	@Override
	public void run() {
		try{
			//cap
			int r=0;
			for(int i=0;i<999999;i++){
				r+=i;
			}
			System.out.println(Thread.currentThread().getName() + " [test success] " + r);
//			logger.info(Thread.currentThread().getName() + " [test success] " + r);
		}catch(Exception e){
			//batch commit error，暂时不处理
			logger.error("batch commit error",e);
			//可以考虑开启异步线程，将error数据回补到es
		}
	}

}
