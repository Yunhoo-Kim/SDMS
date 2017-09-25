package slave;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import sdms.MsgManager;

public class MasterConnectionManager implements Runnable {
	Socket socket;
	MsgManager msg_manager = new MsgManager();
	private static final Logger logger = Logger
			.getLogger(MasterConnectionManager.class.getName());

	public static MasterConnectionManager master_con_manager;

	public static MasterConnectionManager getInstance() {
		return master_con_manager;
	}

	public MasterConnectionManager(Socket sock) {
		this.socket = sock;
		master_con_manager = this;
	}

	public void receiveMsg(JSONObject msg) {
		this.logger.info("Receive Msg : " + msg.toJSONString());
		MasterConnectionWorker worker = new MasterConnectionWorker(msg, this);
		worker.workerExecutor();
	}

	public JSONObject receiveMsg(Socket socket) {
		try {
			return this.msg_manager.receiveMsg(socket);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return new JSONObject();
	}

	public void sendMsg(JSONObject json) {

		try {
			this.msg_manager.sendMsg(json, this.socket);
		} catch (IOException e) {
			SlaveServer slave = SlaveServer.getInstance();
			if (!slave.getElectionFlag())
				slave.startElection();
		}
	}

	public void sendMsg(JSONObject json, Socket socket) {

		try {
			this.msg_manager.sendMsg(json, socket);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void sendMultiple(ArrayList<JSONObject> list, JSONObject message) {

		this.msg_manager.sendMultiple(list, message);
	}

	public void run() {
		this.logger.info("MasterConnectionManager starting....");
		try {
			while (!Thread.currentThread().isInterrupted()) {
				this.receiveMsg(this.msg_manager.receiveMsg(this.socket));
			}
		} catch (IOException e) {
			SlaveServer slave = SlaveServer.getInstance();
			if (!slave.getElectionFlag())
				slave.startElection();
		}
	}
}
