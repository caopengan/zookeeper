package com.cpa.zk.utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMultiLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class TicketSeller {

    public void sell(){
        System.out.println("售票开始：");
        int sleepTime = (int)(Math.random()*2000);
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("售票结束。");
    }

    public void sellTicketWithLock()throws InterruptedException, KeeperException, IOException {
        LockSample lock = new LockSample();
        lock.acquireLock();
        sell();
        lock.releaseLock();
    }

    /**
     * 使用Curator创建分布式锁
     * @param client
     * @throws Exception
     */
    public void soldTickWithLock(CuratorFramework client)throws Exception{
        //创建分布式锁，锁空间的根节点路径为/curator/locks
        InterProcessMutex mutex =new InterProcessMutex(client,"/curator/locks");
        mutex.acquire();
        //获得了锁进行业务流程
        //代表复杂业务逻辑执行了一段时间
        int sleepTime = (int)(Math.random()*2000);
        Thread.sleep(sleepTime);
        //完成业务流程，释放锁
        mutex.release();

    }


    public static void main(String[] args)throws Exception {
        TicketSeller seller  = new TicketSeller();
        for(int i=0;i<1000;i++){
            seller.sellTicketWithLock();
        }

//        CuratorFramework client = CuratorFrameworkFactory.newClient("192.168.4.15:2181", new ExponentialBackoffRetry(1000, 3, Integer.MAX_VALUE));
//        seller.soldTickWithLock(client);
    }



}
