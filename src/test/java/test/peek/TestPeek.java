package test.peek;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import secfox.soc.consumer.filter.Sentry;
import secfox.soc.consumer.filter.TagAfterPeek;

public class TestPeek {
	
	static Logger logger = Logger.getLogger(TestPeek.class);

	public static void main(String[] args) {
		  // Set up a simple configuration that logs on the console.
	     BasicConfigurator.configure();
		List<String> data = new ArrayList<String>();
		long time = 1514562911000L;
		for(int i=0;i<6500;i++){
			time +=1000;
			String json = "{\"act\":35,\"agg_count\":1,\"collector_ip\":\"192.168.37.1\",\"collector_name\":\"lusuo-pc\",\"collector_type\":1,\"custom_s1\":\"未知\",\"custom_s2\":\"未知\",\"custom_s4\":\"ACL7\",\"dev\":\"127.0.0.1\",\"dev_product\":\"绿盟抗拒绝服务攻击系统\",\"dev_type\":4,\"dev_vendor\":\"绿盟\",\"dip\":\"159.226.7.162\",\"downlink_length\":0,\"dport\":0,\"id\":150884073900007,\"msg\":\"Attack: ACL-DROP src=16.63.172.134 dst=159.226.7.162 sport=0 dport=0 flag=ACL7\",\"name\":\"ACL-DROP\",\"occur_time\":1508840739468,\"pri\":3,\"pt\":2,\"raw_id\":0,\"recept_time\":"+time+",\"ret\":0,\"rule_name\":\"/hostile\",\"sip\":\"16.63.172.134\",\"sport\":0,\"system_type\":3,\"systype\":2003,\"uplink_length\":0}";
			data.add(json);
		}
		Sentry.updateBoundary();
		Sentry.enableCheck();
		TagAfterPeek tag = Sentry.peek(data);
		System.out.println(tag.isSlacing());
		if(tag.isSlacing()){
			String[] antecedentData = tag.getSlicingMap().get(Sentry.getAntecedentIndex());
			String[] subsequentData = tag.getSlicingMap().get(Sentry.getSubsequentIndex());
			System.out.println(antecedentData.length);
			System.out.println(subsequentData.length);
			System.out.println(Sentry.format(Sentry.getReceptTime(antecedentData[287])));
			System.out.println(Sentry.format(Sentry.getReceptTime(subsequentData[0])));
		}
		
	}
	
	
	/*public static void main(String[] args) {
		List<String> t = new ArrayList<String>();
		t.add("1");
		t.add("2");
		t.add("3");
		t.add("4");
		t.add("5");
		t.add("6");
		t.add("7");
		t.add("8");
		t.add("9");
		int c = 0;
		for(int i=0;i < t.size();i++){
			if(Integer.parseInt(t.get(i)) < 6 ) continue;
			c = i;
			break;
		}
		System.out.println(t.get(c));
		String[] nt = new String[t.size()];
		nt = t.toArray(nt);
		
		String[] m = Arrays.copyOfRange(nt, 0,c);
		System.out.println(m.length);
		System.out.println(m[c-1]);
		
	}*/
}
