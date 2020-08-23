package com.learning.concurrency;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Producer-Consumer Problem: A multi-process problem where we have a producer process which is producing some data
 * into a buffer and a consumer process which is consuming the data produced from the buffer.
 * Below are some constraints:
 * 1. Producer adds the data into a buffer and it should wait if the buffer is full.
 * 2. In case the buffer is empty consumer should wait before fetching new data.
 * 3. Consumer thread should shutdown once it receives a shutdown signal.
 * <p>
 * Solution:
 * One of the easiest way to solve this problem is to use Blocking Queue to synchronize the producer and
 * consumer processes. Blocking Queue is a special type of queue implementation which supports below operations:
 * 1. Blocks producer thread if queue is full until some space is available.
 * 2. Blocks Consumer thread if queue is empty until some new data is added.
 * <p>
 * In order to shutdown the consumer thread we can produce some kind of special value which acts as the
 * shutdown signal/silver bullet and consumer thread exits once it received it.
 */

public class ProducerConsumerBQ {

    public static void main(String[] args) {
        BlockingQueue<Integer> dataQueue = new ArrayBlockingQueue(10);
        //Starting the producer & consumer threads
        new Thread(new SlowProducer(dataQueue)).start();
        new Thread(new Consumer(dataQueue)).start();
    }
}

//Producer thread which takes a blocking queue and produces some data into it
class Producer implements Runnable {
    BlockingQueue<Integer> queue;

    public Producer(BlockingQueue<Integer> queue) {
        this.queue = queue;
    }

    public void run() {
        for (int i = 0; i < 100; i++) {
            try {
                System.out.println(Thread.currentThread().getName() + "| Producing " + i);
                queue.put(i);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
        }
        try {
            System.out.println("Producer thread shutting down. Sending a shutdown signal to consumer.");
            //Sending -1 as the shutdown signal to consumer
            queue.put(-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

//Consumer thread which takes a blocking queue and removes some element from it
class Consumer implements Runnable {
    BlockingQueue<Integer> queue;

    public Consumer(BlockingQueue<Integer> queue) {
        this.queue = queue;
    }

    public void run() {
        boolean shutdown = false;
        while (!shutdown) {
            try {
                Integer i = queue.take();
                //Check if the producer has send a shutdown signal
                if (i.equals(-1)) {
                    System.out.println("Shutdown signal received.");
                    shutdown = true;
                } else {
                    System.out.println(Thread.currentThread().getName() + "| Consuming " + i);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
        }
        System.out.println("Consumer shutting down");
    }
}

//Producer with added latency which simulates some data production cost
class SlowProducer implements Runnable {
    BlockingQueue<Integer> queue;

    public SlowProducer(BlockingQueue<Integer> queue) {
        this.queue = queue;
    }

    public void run() {
        for (int i = 0; i < 100; i++) {
            try {
                System.out.println(Thread.currentThread().getName() + "| Producing " + i);
                Thread.sleep(500);
                queue.put(i);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
        }
        try {
            System.out.println("Producer thread shutting down. Sending a shutdown signal to consumer.");
            queue.put(-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

//Consumer with added latency which simulates some data consumption cost
class SlowConsumer implements Runnable {
    BlockingQueue<Integer> queue;

    public SlowConsumer(BlockingQueue<Integer> queue) {
        this.queue = queue;
    }

    public void run() {
        boolean shutdown = false;
        while (!shutdown) {
            try {
                Integer i = queue.take();
                if (i.equals(-1)) {
                    System.out.println("Shutdown signal received.");
                    shutdown = true;
                } else {
                    System.out.println(Thread.currentThread().getName() + "| Consuming " + i);
                    Thread.sleep(500);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
        }
        System.out.println("consumer shutting down");
    }
}
