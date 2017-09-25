package master;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import sdms.MsgManager;
import sdms.SDMSNode;
import slave.MasterConnectionManager;

public class MasterListener implements Runnable {
	Socket socket;
	MsgManager msg_manager = new MsgManager();
	private static final Logger logger = Logger.getLogger(MasterListener.class
			.getName());

	public MasterListener(Socket sock) {
		this.socket = sock;
	}

	public void receiveMsg(JSONObject json) {
		MasterWorker worker = new MasterWorker(
				json, this);
		worker.workerExecutor();
	}

	public JSONObject receiveMsg(Socket socket) {
		try {
			return this.msg_manager.receiveMsg(socket);
		} catch (IOException io) {
			
		}
		return new JSONObject();
	}

	public void sendMsg(JSONObject json) {

		try {
			this.msg_manager.sendMsg(json, this.socket);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			SDMSNode sdms = SDMSNode.getInstance();
			sdms.removeSlave(socket);
		}
	}

	public void sendMsg(JSONObject json, Socket socket) {
		try {
			this.msg_manager.sendMsg(json, socket);
		} catch (IOException e) {
		}
	}

	public void sendMultiple(ArrayList<JSONObject> list, JSONObject message) {
		this.msg_manager.sendMultiple(list, message);
	}

	public void run() {
		try {
			while (!Thread.currentThread().isInterrupted()) {
				this.receiveMsg(this.msg_manager.receiveMsg(this.socket));
			}
		} catch (IOException e) {
			e.printStackTrace();
			MasterWorker worker = new MasterWorker(new JSONObject(), this);
			worker.removeSlave(this.socket);
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
	}

}
