package com.cpa.zk.utils;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Zookeeper实现分布式锁
 */
public class LockSample {

    private ZooKeeper zkClient;
    private static final String LOCK_ROOT_PATH = "/Locks";
    private static final String LOCK_NODE_NAME = "Lock_";
    private String lockPath;

    /**
     * 监控lockPath的前一个节点的watcher
     */
    private Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            System.out.println(event.getPath()+" 前锁释放");
            synchronized (this){
                notifyAll();
            }
        }
    };

    public LockSample() throws IOException {
        zkClient = new ZooKeeper("192.168.4.15:2181", 10000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if(event.getState() == Event.KeeperState.Disconnected){
                    System.out.println("失去连接");
                }
            }
        });
    }

    /**
     * 创建锁
     */
    private void createLock()throws InterruptedException,KeeperException{
        //判断根节点书否存在，如果不存在则创建
        Stat exists = zkClient.exists(LOCK_ROOT_PATH, false);
        if(exists ==null){
            zkClient.create(LOCK_ROOT_PATH,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        //创建LOCK_NODE_NAME节点   有序临时节点
        String lockPath = zkClient.create(LOCK_ROOT_PATH+"/"+LOCK_NODE_NAME,Thread.currentThread().getName().getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(Thread.currentThread().getName()+" 锁创建："+lockPath);
        this.lockPath = lockPath;
    }

    /**
     * 试图获取锁
     * @throws InterruptedException
     * @throws KeeperException
     */
    private void attempLock()throws InterruptedException,KeeperException{
        //获取locks下所有子节点，按照序号排序
        List<String> lockPaths = null;
        lockPaths = zkClient.getChildren(LOCK_ROOT_PATH,false);
        Collections.sort(lockPaths);
        int index = lockPaths.indexOf( lockPath.substring(LOCK_ROOT_PATH.length() + 1));
        //如果lockPath是序号最小的节点，则获取锁
        if(index == 0){
            System.out.println(Thread.currentThread().getName()+"获得锁，lockPath:"+lockPath);
            return;
        }else {
            //lockPath不是序号最小的节点，监控前一个节点
            String preLockPath = lockPaths.get(index-1);
            Stat exists = zkClient.exists(LOCK_ROOT_PATH + "/" +preLockPath, watcher);
            if(exists == null){
                attempLock();
            }else {
                System.out.println("等待前锁释放，preLockPath："+preLockPath);
                synchronized (watcher){
                    watcher.wait();
                }
                attempLock();
            }
        }
    }

    /**
     * 释放锁
     */
    public void releaseLock()throws InterruptedException, KeeperException {
        zkClient.delete(lockPath,-1);
        zkClient.close();
        System.out.println("锁释放："+lockPath);
    }

    public void acquireLock()throws  InterruptedException,KeeperException{
        //创建锁
        createLock();
        //试图获取锁
        attempLock();
    }



}
