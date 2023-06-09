package simpledb.buffer;

import java.util.*;

import simpledb.file.*;
import simpledb.log.LogMgr;


public class MidInsBufferMgr implements BufferMgr {
   /**
    * Constructor:  Creates a buffer manager having the specified 
    * number of buffer slots.
    * This constructor depends on a {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} object.
    * @param numbuffs the number of buffer slots to allocate
    */
   private int numAvailable = 0;
   private int numOfFreeBuf = 0;
   private int numOfUnpinBuf = 0;
   private static final long MAX_TIME = 10000;

   public ArrayList<Buffer> freeList;
   public LinkedList<Buffer> lruList;
   public HashMap<BlockId, Buffer> allocatedBuffer;
   public int numbuffs;
   // public boolean isFull = false;

   public int hitCnt = 0;
   public int referenceCnt = 0;

   public MidInsBufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {
        /*Write your code */
      
      this.numbuffs = numbuffs;
      freeList = new ArrayList<>();
      lruList = new LinkedList<>();
      allocatedBuffer = new HashMap<BlockId, Buffer>();
      for(int i = 0; i < numbuffs; i++){
         Buffer buff = new Buffer(fm, lm, i);
         freeList.add(buff);
      }
      numOfFreeBuf = freeList.size();
      numAvailable = numOfFreeBuf + numOfUnpinBuf;
   }

   public synchronized int available() {
      /* Write your code*/
      numOfFreeBuf = freeList.size();
      // int cnt = 0;
      // for(BlockId key : allocatedBuffer.keySet()){
      //    Buffer buff = allocatedBuffer.get(key);
      //    if(!buff.isPinned()){
      //       cnt++;
      //    }
      // }
      // numOfUnpinBuf = cnt;
      numAvailable = numOfFreeBuf + numOfUnpinBuf;
      return numAvailable;
   }

   public synchronized void flushAll(int txnum){
      /* Write your code*/
      for(BlockId key : allocatedBuffer.keySet()){
         Buffer buff = allocatedBuffer.get(key);
         if (buff.modifyingTx() == txnum)
            buff.flush();
      }
      while(!lruList.isEmpty()){
         Buffer buff = lruList.removeLast();
         while(buff.isPinned()){
            buff.unpin();
         }
         freeList.add(buff);
      }
      allocatedBuffer.clear();
      numAvailable = numbuffs;
      // isFull = false;
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
   
   public synchronized void unpin(Buffer buff) {
      /* Write your code */
      buff.unpin();
      if (!buff.isPinned()) {
         numOfUnpinBuf++;
         notifyAll();
      }
   }

   public void printStatus(){
      /* Write your code */
      System.out.println("Allocated Buffers:");
      LinkedList<Buffer> copyList = new LinkedList<>(lruList);
      while (!copyList.isEmpty()){
         Buffer buff = copyList.removeFirst();
         if(buff.isPinned()){
            System.out.println("Buffer "+buff.getID()+": "+buff.block().toString()+" pinned");
         }
         else{
            System.out.println("Buffer "+buff.getID()+": "+buff.block().toString()+" unpinned"); 
         }
      }
   } 

   public float getHitRatio(){
        /* Write your code */
        float ratio = (float)hitCnt / (float)referenceCnt;
        double result = Math.round(ratio*10000)/100.0;
        return (float) result;
   }

   private boolean waitingTooLong(long starttime) {
      return System.currentTimeMillis() - starttime > MAX_TIME;
   }

   private Buffer tryToPin(BlockId blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         if(numOfFreeBuf != 0){
            Buffer tempbuff = freeList.remove(0);
            tempbuff.assignToBlock(blk);
            allocatedBuffer.put(blk,tempbuff);
            if(lruList.size() <= 3)
               lruList.addFirst(tempbuff);
            else{
               lruList.add((int)Math.floor(lruList.size() * 5 / 8),tempbuff);
            }
            buff = tempbuff;
            numOfFreeBuf--;
         }
         else{
            BlockId temp = chooseUnpinnedBuffer();
            if (temp == null){
               return null;
            }
            buff = allocatedBuffer.get(temp);
            buff.assignToBlock(blk);
            allocatedBuffer.remove(temp);
            allocatedBuffer.put(blk,buff);
            lruList.removeLast();
            if(lruList.size() <= 3)
               lruList.addFirst(buff);
            else{
               lruList.add((int)Math.floor(lruList.size() * 5 / 8),buff);
            }
         }
      }
      if (!buff.isPinned())
         numOfUnpinBuf--;
      buff.pin();
      return buff;
   }

   private Buffer findExistingBuffer(BlockId blk) {
      Buffer buff = allocatedBuffer.get(blk);
      if(buff != null){ // find page
         hitCnt++;
         lruList.remove(buff);
         lruList.addFirst(buff);
         return buff;
      }
      return null;
   }

   private BlockId chooseUnpinnedBuffer() {
      ListIterator<Buffer> iterator = lruList.listIterator(lruList.size());
      while (iterator.hasPrevious()) {
         Buffer data = iterator.previous();
         if (!data.isPinned()) {
            data.flush();
            return data.block();
         }
      }
      return null;
   }
   
}
