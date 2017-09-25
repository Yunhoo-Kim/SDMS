package slave;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import sdms.SDMSNode;

public class SlaveWorker {
	JSONObject message = null;
	SlaveListener listener = null;
	SlaveServer slave_server = null;
	private static final Logger logger = Logger.getLogger(SlaveWorker.class
			.getName());

	public SlaveWorker(JSONObject message, SlaveListener listener) {
		this.message = message;
		this.listener = listener;
		this.slave_server = SlaveServer.getInstance();

	}

	public void workerExecutor() {
		String op_type = (String) this.message.get("op_type");
		
		this.logger.info("Op type : " + op_type);
		
		switch (op_type) {
		case "remove topic":
			this.removeTopic();
			break;
		case "add topic":
			this.addTopic();
			break;
		case "add Client":
			this.addClient();
			break;
		case "remove Client":
			this.removeClient();
			break;
		case "publish":
			this.publishToMaster();
			break;
		case "start election":
			this.startElection();
			break;
		case "i am bully":
			this.bullySelected();
			break;
		}
	}

	private void bullySelected(){
		String ip = (String)this.message.get("ip");
		int node_id = ((Number)this.message.get("node_id")).intValue();
		int port = ((Number)this.message.get("port")).intValue();
		this.logger.info("Bully is " + " " + ip + " " + node_id + " " + port);
		
		this.slave_server.setMaster(ip, port, node_id);
		SDMSNode.getInstance().heartbeat_thread.interrupt();
		this.slave_server.master_thread.interrupt();
		this.slave_server.registerToMaster();
		this.slave_server.election_end_flag = true;
		this.slave_server.setElectionFlag(false);
		this.slave_server.election_cnt = 0;
		this.logger.info("After reconnect with master");
//		try {
////			this.listener.socket.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
	
	public void publishToMaster() {
		this.logger.info("publish msg from client"
				+ this.message.toJSONString());
		this.message.put("op_type", "publish from slave");
		MasterConnectionManager master = MasterConnectionManager.getInstance();
		master.sendMsg(this.message);

	}

	public void startElection() {
		if (!this.slave_server.getElectionFlag())
			this.slave_server.startElection();
		
		try {
			this.listener.socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void addClient() {
		int client_id = ((Number) this.message.get("client_id")).intValue();
		logger.info("clietn ID : " + client_id + " Connected");
		this.slave_server.addClient(this.listener.socket, client_id);
	}

	private void removeClient() {
		int client_id = ((Number) this.message.get("client_id")).intValue();
		logger.info("clietn ID : " + client_id + " disconnected");
		this.slave_server.removeClient(client_id);
		try {
			this.listener.socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void addTopic() {
		String topic = (String) this.message.get("topic");
		int client_id = ((Number) this.message.get("client_id")).intValue();
		logger.info("clietn ID : " + client_id + " add topic " + topic);
		this.slave_server.addTopic(client_id, topic);
	}

	private void removeTopic() {
		String topic = (String) this.message.get("topic");
		int client_id = ((Number) this.message.get("client_id")).intValue();
		logger.info("clietn ID : " + client_id + " remove topic " + topic);
		this.slave_server.removeTopic(client_id, topic);
	}

}
