package slave;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import sdms.SDMSNode;

public class MasterConnectionWorker {
	JSONObject message = null;
	MasterConnectionManager listener = null;
	SlaveServer slave_server = null;
	private static final Logger logger = Logger
			.getLogger(MasterConnectionManager.class.getName());
	
	public MasterConnectionWorker(JSONObject message,
			MasterConnectionManager listener) {
		this.message = message;
		this.listener = listener;
		this.slave_server = SlaveServer.getInstance();
	}

	public void workerExecutor() {
		String op_type = (String) this.message.get("op_type");
		this.logger.info("op_type : " + op_type);
		switch (op_type) {
		case "slave update":
			this.updateSlave();
			break;
//		case "slave":
//			this.removeSlave();
//			break;
		case "publish from master":
			this.publishToClient();
			;
			break;
		}
	}

	public void publishToClient() {
		this.logger.info("Get message from Master");
		this.msgToClients(this.message);
	}

	public void startElection() {

	}

	private void msgToClients(JSONObject message) {
		this.logger.info("Msg to Client");
		ArrayList<JSONObject> has_topic_clients = new ArrayList<JSONObject>();
		ArrayList<JSONObject> client_list = this.slave_server.getClientList();
		String topic = (String) message.get("topic");
		Iterator<JSONObject> iter = client_list.iterator();
		while (iter.hasNext()) {
			JSONObject temp = iter.next();
			ArrayList<String> topic_list = (ArrayList<String>) temp
					.getOrDefault("topic_list", new ArrayList<String>());
			this.logger.info("topic list is : " + topic_list.toString());
			if (topic_list.contains(topic)) {
				this.logger.info("has topic client");
				has_topic_clients.add(temp);
			}
		}
		this.logger.info(has_topic_clients.toString());
		this.listener.sendMultiple(has_topic_clients, message);
	}

	private void addSlave(JSONObject json) {
		SDMSNode sdms = SDMSNode.getInstance();
		String ip = (String) json.get("ip");
		int port = ((Number) json.get("port")).intValue();
		int id = ((Number) json.get("node_id")).intValue();
		int weight = ((Number) json.get("weight")).intValue();
		boolean eligible = (boolean) json.get("eligible");
		sdms.addSlave(ip, port, id, weight, eligible, this.listener.socket);
	}
	
	private void updateSlave(){
		SDMSNode sdms = SDMSNode.getInstance();
		ArrayList<JSONObject> list = sdms.getSlaveList();
		list.clear();
		list = (ArrayList<JSONObject>)this.message.get("slave_list");
		this.logger.info("Slave List : " + list.toString());
		Iterator<JSONObject> iter = list.iterator();
		while(iter.hasNext()){
			this.addSlave(iter.next());
		}
		
		
	}
	private void removeSlave() {
		SDMSNode sdms = SDMSNode.getInstance();
		int id = ((Number) this.message.get("node_id")).intValue();
		sdms.removeSlave(id);
	}
}
