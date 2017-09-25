package queue;

import java.util.LinkedList;

public class SimpleQueue<E> {
	private LinkedList<E> list = new LinkedList<E>();
	public void enqueue(E o){
		list.addLast(o);
	}
	public E dequeue(){
		if(list.isEmpty()){
			return null;
		}
		return list.removeFirst();
	}
	public boolean isEmpty(){
		return list.isEmpty();
	}
	public int size(){
		return list.size();
	}
}
