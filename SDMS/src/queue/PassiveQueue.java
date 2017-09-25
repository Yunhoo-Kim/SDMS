package queue;

public class PassiveQueue<E> {
	SimpleQueue<E> queue = new SimpleQueue<E>();
	public synchronized void accept(E r){
		queue.enqueue(r);
		notify();
	}
	public synchronized E release(){
		for(;;){
			if(queue.isEmpty()){
				try{
					wait();
				}catch(InterruptedException e){
					e.getStackTrace();
				}
			}else{
				return queue.dequeue();
			}
		}
	}
}
