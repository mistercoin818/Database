package simpledb.buffer;

import java.util.*;

import simpledb.file.*;
import simpledb.log.LogMgr;


public class LRUBufferMgr implements BufferMgr {
   // private Buffer[] bufferpool; /* buffer pool */ 
   private int numAvailable;   /* the number of available (unpinned) buffer slots */
   private static final long MAX_TIME = 10000; /* 10 seconds */

   public ArrayList<Buffer> unpinnedBuffer;
   public HashMap<BlockId, Buffer> allocatedBuffer;

   public int hitCnt = 0;
   public int referenceCnt = 0;

   /**
    * Constructor:  Creates a buffer manager having the specified 
    * number of buffer slots.
    * This constructor depends on a {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} object.
    * @param numbuffs the number of buffer slots to allocate
    */
    public LRUBufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {
        /*Write your code */
      numAvailable = numbuffs;
      unpinnedBuffer = new ArrayList<>();
      allocatedBuffer = new HashMap<BlockId, Buffer>();
      for (int i=0; i<numbuffs; i++){
         Buffer buff = new Buffer(fm, lm, i);
         unpinnedBuffer.add(buff);
      }
   }

   public synchronized int available() {
      /* Write your code*/
        return numAvailable;
   }

   public synchronized void flushAll(int txnum){
      /* Write your code*/
      for(BlockId key : allocatedBuffer.keySet()){
         Buffer buff = allocatedBuffer.get(key);
         if(buff.modifyingTx() == txnum){
            buff.flush();
         }
      }
   }

   public synchronized Buffer pin(BlockId blk) {
      /* Write your code*/
      try {
         referenceCnt++;
         long timestamp = System.currentTimeMillis();
         Buffer buff = tryToPin(blk);
         while (buff == null && !waitingTooLong(timestamp)) {
            wait(MAX_TIME);
            buff = tryToPin(blk);
         }
         if (buff == null)
            throw new BufferAbortException();
         return buff;
      }
      catch(InterruptedException e) {
         throw new BufferAbortException();
      }
   }
   
   private boolean waitingTooLong(long starttime) {
      return System.currentTimeMillis() - starttime > MAX_TIME;
   }
   
   public synchronized void unpin(Buffer buff) {
      /* Write your code */
      buff.unpin();
      if (!buff.isPinned()) {
         numAvailable++;
         unpinnedBuffer.add(buff);
         notifyAll();
      }
   }

   private Buffer tryToPin(BlockId blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         Buffer tempbuff = chooseUnpinnedBuffer();
         if (tempbuff == null)
            return null;
         for(BlockId key : allocatedBuffer.keySet()){
            if(allocatedBuffer.get(key).equals(tempbuff)){
               allocatedBuffer.remove(key);
               break;
            }
         }
         buff = tempbuff;
         buff.assignToBlock(blk);
         allocatedBuffer.put(blk, buff);

      }
      if (!buff.isPinned()) // pinnum ==0 인 거 다시 pin
         numAvailable--;
      buff.pin();
      return buff;
   }

   private Buffer findExistingBuffer(BlockId blk) {
      Buffer buff = allocatedBuffer.get(blk);
      if(buff != null){
         hitCnt++;
         if(!buff.isPinned()){
            unpinnedBuffer.remove(unpinnedBuffer.indexOf(buff));
         }
         return buff;
      }
      return null;
   }

   private Buffer chooseUnpinnedBuffer() {
      if(unpinnedBuffer.size() != 0){
         Buffer buff = unpinnedBuffer.remove(0);
         return buff;
      }
      return null;
   }

   public void printStatus(){
      /* Write your code */
      System.out.println("Allocated Buffers:");
      for(BlockId key : allocatedBuffer.keySet()){
         Buffer buff = allocatedBuffer.get(key);
         System.out.printf("Buffer %d: ", buff.getID());
         System.out.printf("%s %s\n", buff.block().toString(), buff.isPinned()?"pinned":"unpinned");     
      }
      System.out.printf("%s", "Unpinned Buffers in LRU order: ");
      for(Buffer buff : unpinnedBuffer){
         System.out.printf("%s ", buff.getID());
      }
      System.out.println(" ");
   }

   public float getHitRatio(){
        /* Write your code */
        float ratio = (float)hitCnt / (float)referenceCnt;
        double result = Math.round(ratio*10000)/100.0;
        return (float) result;
    }

}

