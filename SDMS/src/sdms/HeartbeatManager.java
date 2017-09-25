package sdms;
import java.io.IOException;
import java.util.ArrayList;

import org.json.simple.JSONObject;

import slave.MasterConnectionManager;
import slave.SlaveServer;

public class HeartbeatManager implements Runnable {

	SDMSNode sdms;

	public HeartbeatManager() {
		this.sdms = SDMSNode.getInstance();
	}

	public void run() {
		while (!Thread.currentThread().isInterrupted()) {
			if (this.sdms.getMasterFlag()) {
				// master
				this.masterHeartBeat(this.sdms.heartbeat_queue.release());
			} else {
				// slave
				this.slaveHeartBeat(this.sdms.heartbeat_queue.release());
			}
		}
		this.sdms.logger.info("heart beat thread is exited");
	}
	
	private void masterHeartBeat(JSONObject json){
		this.sdms.logger.info("HeartBeat json : " + json.toJSONString());
		// update slave heart beat class and return heartbeat signal
		SDMSNode sdms = SDMSNode.getInstance();
//		this.sdms.logger.info("Update slave connections");
		ArrayList<JSONObject> slave_list = sdms.getSlaveList();
		for(int i=0;i<slave_list.size();i++){
			if(((Number)slave_list.get(i).get("node_id")).intValue() == ((Number)json.get("node_id")).intValue()){
				slave_list.get(i).put("connections", ((Number)json.get("connections")).intValue());
				return;
			}
		}
	}
	
	private void slaveHeartBeat(JSONObject json){
		// send heartbeat to master
		MasterConnectionManager master_manager = MasterConnectionManager.getInstance();
//		JSONObject heart_json = new JSONObject();
//		SDMSNode sdms = SDMSNode.getInstance();
		SlaveServer slave = SlaveServer.getInstance();
		json.put("connections", slave.getClientConnections());
		//send heartbeat
//		this.sdms.logger.info("send heartbeat");
		master_manager.sendMsg(json);
	}

}
