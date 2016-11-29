package mycat.leaderus.lzy.cachesys.memcache_v5;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by 行知道人 on 2016/11/29.
 */
public class ManagerMemory {
    private static LinkedBlockingQueue<Chunk>[] used = new LinkedBlockingQueue[MemConfig.SLAB_SIZE/MemConfig.CHUNK_SIZES];
    private static LinkedBlockingQueue<Chunk>[] empty = new LinkedBlockingQueue[MemConfig.SLAB_SIZE/MemConfig.CHUNK_SIZES];

    static {
        for (int i = 0; i < used.length; i++) {
            used[i]= new LinkedBlockingQueue<>();
            empty[i] = new LinkedBlockingQueue<>();
        }
        new Thread(){

            @Override
            public void run() {
                while (true){
                    Iterator<Chunk> tmp = null;
                    Chunk tmpChunk = null;
                    for (int i = 0; i < used.length ; i++) {
                        tmp = used[i].iterator();
                        while(tmp.hasNext()){
                            tmpChunk = tmp.next();
                            if (tmpChunk.getTimeout()<System.currentTimeMillis())
                                removeChunk(tmpChunk);
                        }
                    }
                    try {
                        sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();
    }

    public static Chunk getChunk(int size){
        int index = (size-1)/MemConfig.CHUNK_SIZES;
        Chunk tmpChunk = null;
        if(empty[index].size()==0) {
            Slab tmp = SlabClass.getSlab();
            if (tmp != null)
                empty[index].addAll(Arrays.asList(tmp.init((index+1) * MemConfig.CHUNK_SIZES).getChunks()));
        }
        if(empty[index].size()!=0) {
            tmpChunk = empty[index].remove();
            used[index].add(tmpChunk);
        }else {
            throw new RuntimeException("内存暂时不足");
        }
        return tmpChunk;
    }

    public static void removeChunk(Chunk e){
        int index = e.getByteBuffer().capacity()/MemConfig.CHUNK_SIZES-1;
        ReadWritePool.remove(e.getKey());
        used[index].remove(e);
        empty[index].add(e);
    }
}