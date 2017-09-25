package master;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import queue.HeartBeatQueue;
import sdms.MsgManager;
import sdms.SDMSNode;

public class MasterWorker {
	JSONObject message = null;
	MasterListener listener = null;
	private static final Logger logger = Logger.getLogger(MasterWorker.class
			.getName());

	public MasterWorker(JSONObject message, MasterListener listener) {
		this.message = message;
		this.listener = listener;
	}

	public void workerExecutor() {
		String op_type = (String) this.message.get("op_type");
		switch (op_type) {
		case "slave register":
			this.addSlave();
			break;
		case "slave unregister":
			this.removeSlave();
			break;
		case "forward client":
			this.clientForwarding();
			break;
		case "publish from slave":
			this.receiveMsgFromSlave();
			break;
		case "publish from master":
			this.receiveMsgFromOtherMaster();
			break;

		case "heartbeat":
			this.receiveHeartbeat();
			break;
		}
	}

	public void clientForwarding() {
		MasterServer master = MasterServer.getInstance();
		JSONObject slave_data = master.loadBalancing();
		slave_data.put("op_type", "return client forwarding");
		this.listener.sendMsg(slave_data);
	}

	public ArrayList<JSONObject> getMasterList() {
		SDMSNode sdms = SDMSNode.getInstance();
		ArrayList<JSONObject> master_list = null;
		try {
			Socket socket = new Socket(sdms.getBrokerIP(), sdms.getBrokerPort());
			JSONObject request = new JSONObject();
			JSONObject response = new JSONObject();
			this.logger.info("********Master List Request");
			request.put("op_type", "get masters");
			this.listener.sendMsg(request, socket);
			response = this.listener.receiveMsg(socket);
			this.logger.info("********Master List " + response.toJSONString());
			master_list = (ArrayList<JSONObject>) response.get("master_list");

			return master_list;
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return master_list;
	}

	public ArrayList<JSONObject> getSlaveList() {
		SDMSNode sdms = SDMSNode.getInstance();
		ArrayList<JSONObject> slave_list = sdms.getSlaveList();
		return slave_list;
	}

	public void msgToSlaves(JSONObject message) {
		ArrayList<JSONObject> slave_list = this.getSlaveList();
		this.logger.info("After : " + slave_list.toString());
		this.listener.sendMultiple(slave_list, message);
	}

	public void msgToOtherMasters(JSONObject message) {
		SDMSNode sdms = SDMSNode.getInstance();
		ArrayList<JSONObject> master_list = this.getMasterList();
		ArrayList<JSONObject> master_socket_list = new ArrayList<JSONObject>();
		Iterator<JSONObject> iter = master_list.iterator();
		this.logger.info("Send Message To other masters");
		if (master_list.size() == 1) {
			return;
		}

		MsgManager msg_manager = new MsgManager();
		msg_manager.sendMultipleWithoutSocket(master_list, message);
	}

	public void receiveMsgFromSlave() {
		this.logger.info("Get Message from Slave");
		this.publish();
	}

	public void receiveMsgFromOtherMaster() {
		JSONObject msg_to_masters = this.message;
		msg_to_masters.put("op_type", "publish from master");
		this.msgToSlaves(msg_to_masters);
	}

	private void publish() {
		this.logger.info("start publish");
		JSONObject msg_to_masters = this.message;
		JSONObject msg_to_slaves = this.message;
		msg_to_masters.put("op_type", "publish from master");
		msg_to_slaves.put("op_type", "publish from master");
		this.msgToOtherMasters(msg_to_masters);
		this.msgToSlaves(msg_to_slaves);
		this.logger.info("end publish");

	}

	private void receiveHeartbeat() {
		// input heartbeat queue
//		this.logger.info("Receive HeartBeat from slave "
//				+ this.message.toJSONString());
		SDMSNode sdms = SDMSNode.getInstance();
		sdms.heartbeat_queue.accept(this.message);
	}

	private void addSlave() {
		SDMSNode sdms = SDMSNode.getInstance();
		String ip = (String) this.message.get("ip");
		int port = ((Number) this.message.get("port")).intValue();
		int id = ((Number) this.message.get("node_id")).intValue();
		int weight = ((Number) this.message.get("weight")).intValue();
		boolean eligible = (boolean) this.message.get("eligible");
		// System.out.println(ip + " " + port + " " + id + " " + weight+ " " +
		// eligible);
		sdms.addSlave(ip, port, id, weight, eligible, this.listener.socket);
		
		JSONObject json = new JSONObject();
		this.logger.info("Before : " + this.getSlaveList().toString());
		ArrayList<JSONObject> slave_list = new ArrayList<JSONObject>();
		slave_list = (ArrayList<JSONObject>)this.getSlaveList().clone();
		
		Iterator<JSONObject> iter = slave_list.iterator();
		ArrayList<JSONObject> new_list = new ArrayList<JSONObject>();
		while(iter.hasNext()){
			JSONObject temp = (JSONObject)iter.next().clone();
			temp.remove("socket");
			new_list.add(temp);
		}
		
		json.put("slave_list", new_list);
		json.put("op_type", "slave update");
		this.logger.info("After Slave list : " + json.toJSONString());
		this.msgToSlaves(json);
	}
	public void removeSlave(Socket socket){

		SDMSNode sdms = SDMSNode.getInstance();
		sdms.removeSlave(socket);
		JSONObject json = new JSONObject();
		this.logger.info("Before : " + this.getSlaveList().toString());
		ArrayList<JSONObject> slave_list = new ArrayList<JSONObject>();
		slave_list = (ArrayList<JSONObject>)this.getSlaveList().clone();
		
		Iterator<JSONObject> iter = slave_list.iterator();
		ArrayList<JSONObject> new_list = new ArrayList<JSONObject>();
		while(iter.hasNext()){
			JSONObject temp = (JSONObject)iter.next().clone();
			temp.remove("socket");
			new_list.add(temp);
		}
		
		json.put("slave_list", new_list);
		json.put("op_type", "slave update");
		this.logger.info("After Slave list : " + json.toJSONString());
		this.msgToSlaves(json);
		
	}
	private void removeSlave() {
		SDMSNode sdms = SDMSNode.getInstance();
		int id = ((Number) this.message.get("node_id")).intValue();
		sdms.removeSlave(id);
		JSONObject json = new JSONObject();
		ArrayList<JSONObject> slave_list = (ArrayList<JSONObject>)this.getSlaveList().clone();
		Iterator<JSONObject> iter = slave_list.iterator();
		
		while(iter.hasNext()){
			iter.next().remove("socket");
		}
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
		}
		json.put("slave_list", slave_list);
		json.put("op_type", "slave update");
		this.msgToSlaves(json);
	}

}
