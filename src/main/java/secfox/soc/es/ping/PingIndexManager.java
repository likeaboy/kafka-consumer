package secfox.soc.es.ping;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PingIndexManager {

	private final static Logger logger = LoggerFactory
			.getLogger(PingIndexManager.class);

	private final static String IDX_PING = "ping";
	
	private static volatile PingIndexManager instance = new PingIndexManager();
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:ss:mm");
	private TransportClient client;
	
	public static PingIndexManager getInstance(){
		return  instance;
	}
	
	public boolean ping(){
		XContentBuilder jsonBuilder;
		try {
			jsonBuilder = XContentFactory.jsonBuilder();
			String formatDate = sdf.format(new Date());
			IndexResponse response = this.client
					.prepareIndex(IDX_PING, "test")
					.setSource(
							jsonBuilder.startObject()
									.field("from", "es-insert")
									.field("pingDate", new Date())
									.field("message", "ping es...").endObject())
					.get();
			logger.info("[PingIndexManager ping es...,ping time : {}]",formatDate);
			return true;
		} catch (IOException e) {
			logger.error("", e);
			return false;
		} catch (Exception e2) {
			logger.error("",e2);
			return false;
		}
	}

	public void initPingIndex(TransportClient client) {
		this.client = client;
		if (isExistPingIndex(client))
			return;
		logger.info("[PingIndexManager create 'ping' index]");
		ping();
	}

	public boolean isExistPingIndex(TransportClient client){
		try{
			ActionFuture<IndicesStatsResponse> isr = client.admin().indices()
					.stats(new IndicesStatsRequest().all());
			Set<String> indexes = isr.actionGet().getIndices().keySet();
			return indexes.contains(IDX_PING);
		}catch(Exception e){
			logger.error("[es status is not alive...]",e);
			return true;
		}
	}
}
