package secfox.soc.consumer.filter;

import java.util.List;
import java.util.Map;

public class TagAfterPeek {
	private String index;
	private List<String> data;
	private boolean isSlacing = false;
	private Map<String,String[]> slicingMap;
	
	public TagAfterPeek(String index,List<String> data){
		this.index = index;
		this.data = data;
	}
	
	public TagAfterPeek(Map<String,String[]> slicingMap,boolean isSlacing){
		this.slicingMap = slicingMap;
		this.isSlacing = isSlacing;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public List<String> getData() {
		return data;
	}

	public void setData(List<String> data) {
		this.data = data;
	}

	public boolean isSlacing() {
		return isSlacing;
	}

	public void setSlacing(boolean isSlacing) {
		this.isSlacing = isSlacing;
	}

	public Map<String, String[]> getSlicingMap() {
		return slicingMap;
	}

	public void setSlicingMap(Map<String, String[]> slicingMap) {
		this.slicingMap = slicingMap;
	}
	
}
