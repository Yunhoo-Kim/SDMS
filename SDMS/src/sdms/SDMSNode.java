package sdms;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Timer;

import master.MasterServer;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import queue.HeartBeatQueue;
import slave.HeartBeatWorker;
import slave.SlaveServer;

public class SDMSNode {
	ArrayList<JSONObject> slave_connections = new ArrayList<JSONObject>();
	ArrayList<JSONObject> slave_table = new ArrayList<JSONObject>();
	Thread sdms_thread;
	public Thread heartbeat_thread;
	HeartBeatWorker heartbeat_worker_thread;
	public Timer heartbeat_timer;
	public ServerSocket server_socket;
	public HeartBeatQueue heartbeat_queue = new HeartBeatQueue();
	boolean master_flag;
	boolean is_bully = false;
	String ip;
	int port;
	int id;
	int weight;
	String broker_ip;
	int broker_port;

	public static SDMSNode sdms_node;
	public Logger logger = Logger.getLogger(SDMSNode.class.getName());

	public static SDMSNode getInstance() {
		return sdms_node;
	}

	public SDMSNode(String broker_ip, int broker_port, int port,
			boolean master_flag) {
		this.broker_ip = broker_ip;
		this.broker_port = broker_port;
		this.port = port;
		InetAddress IP = null;
		try {
			IP = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
		this.setMasterFlag(master_flag);
		this.ip = IP.getHostAddress();
		this.logger.info("Available Processors : " +Runtime.getRuntime().availableProcessors());
		this.weight = Runtime.getRuntime().availableProcessors();
		this.id = (int) (System.currentTimeMillis() / 1000);
		sdms_node = this;
	}

	public void setMasterFlag(boolean flag) {
		this.master_flag = flag;
	}

	public boolean getMasterFlag() {
		return this.master_flag;
	}

	public void startServer() {
		this.runServer();
	}

	public void runServer() {
		if (this.getMasterFlag()) {
			this.sdms_thread = new Thread(new MasterServer());
			this.sdms_thread.start();
		} else {
			this.sdms_thread = new Thread(new SlaveServer());
			this.sdms_thread.start();
		}
	}

	public void runAsMaster(int master_id) {
		if (master_id == 0) {
			this.setMasterFlag(true);
			this.startServer();
			return;
		}
		try {
			this.server_socket.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			// e1.printStackTrace();
		}
		this.heartbeat_thread.interrupt();
		this.setMasterFlag(true);

		this.logger.info("Start Server");
		try {
			Socket socket = new Socket(this.getBrokerIP(), this.getBrokerPort());
			MsgManager msg_manager = new MsgManager();
			JSONObject json = this.getMyInfo();
			json.put("op_type", "update master");
			json.put("prev_id", master_id);
			msg_manager.sendMsg(json, socket);
		} catch (UnknownHostException e) {
			// e.printStackTrace();
		} catch (IOException e) {
			// e.printStackTrace();
		}
		this.slave_table.clear();
		this.logger.info("Start Server2");
		this.startServer();

	}

	public void startHeartbeatManager() {
		if (!this.getMasterFlag()) {
			Timer timer = new Timer();
			long delay = 0;
			long period = 1000;
			this.heartbeat_worker_thread = new HeartBeatWorker(
					this.heartbeat_queue);
			timer.scheduleAtFixedRate(this.heartbeat_worker_thread, delay,
					period);
			this.heartbeat_timer = timer;
		}
		this.heartbeat_thread = new Thread(new HeartbeatManager());
		this.heartbeat_thread.start();
	}

	public void addSlave(String ip, int port, int id, int weight,
			boolean eligible, Socket socket) {
		JSONObject slave = new JSONObject();
		slave.put("ip", ip);
		slave.put("node_id", id);
		slave.put("port", port);
		slave.put("weight", weight);
		slave.put("eligible", eligible);
		slave.put("socket", socket);
		slave.put("connections", 0);
		this.slave_table.add(slave);
		System.out.println("add slave!");
	}

	public void removeSlave(int id) {
		Iterator<JSONObject> iter = this.slave_table.iterator();
		while (iter.hasNext()) {
			JSONObject temp = iter.next();
			if (((Number) temp.get("node_id")).intValue() == id) {
				this.slave_table.remove(temp);
			}
		}
	}

	public void removeSlave(Socket sock) {
		Iterator<JSONObject> iter = this.slave_table.iterator();

		while (iter.hasNext()) {
			JSONObject temp = iter.next();
			if ((Socket) temp.get("socket") == sock) {
				this.slave_table.remove(temp);
				break;
			}
		}
	}

	public class WeightComparator implements Comparator<JSONObject> {

		@Override
		public int compare(JSONObject o1, JSONObject o2) {
			// TODO Auto-generated method stub
			Integer weight1 = ((Number) o1.get("weight")).intValue();
			Integer weight2 = ((Number) o2.get("weight")).intValue();
			return weight1.compareTo(weight2);
		}

	}

	public ArrayList<JSONObject> getSlaveList() {
		return this.slave_table;
	}

	public String getBrokerIP() {
		return this.broker_ip;
	}

	public int getBrokerPort() {
		return this.broker_port;
	}

	public int getPort() {
		return this.port;
	}

	public int getId() {
		return this.id;
	}

	public int getWeight() {
		return this.weight;
	}

	public String getIP() {
		return this.ip;
	}

	public void setIsBully(boolean flag) {
		this.is_bully = flag;
	}

	public boolean getIsBully() {
		return this.is_bully;
	}

	public JSONObject getMyInfo() {
		JSONObject json = new JSONObject();
		json.put("node_id", this.id);
		json.put("ip", this.ip);
		json.put("port", this.port);
		json.put("weight", this.weight);
		return json;
	}
}
