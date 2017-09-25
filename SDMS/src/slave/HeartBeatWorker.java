package slave;

import java.util.TimerTask;

import org.json.simple.JSONObject;

import queue.HeartBeatQueue;
import sdms.SDMSNode;

public class HeartBeatWorker extends TimerTask{
	HeartBeatQueue heartbeat_queue;
	SDMSNode sdms = SDMSNode.getInstance();
	TimerTask task;
	
	public HeartBeatWorker(HeartBeatQueue queue) {
		// TODO Auto-generated constructor stub
		this.heartbeat_queue = queue;
		
	}
	
	public void run(){
		JSONObject heartbeat = new JSONObject();
		heartbeat = (JSONObject)this.sdms.getMyInfo().clone();
		heartbeat.put("op_type", "heartbeat");
		this.heartbeat_queue.accept(heartbeat);
	}
	
	
}
