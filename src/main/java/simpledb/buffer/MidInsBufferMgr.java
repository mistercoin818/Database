package simpledb.buffer;

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
   public MidInsBufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {
        /*Write your code */
   }

   public synchronized int available() {
      /* Write your code*/
        return 0;
   }

   public synchronized void flushAll(int txnum){
      /* Write your code*/
   }

   public synchronized Buffer pin(BlockId blk) {
      /* Write your code*/
      return null;
   }
   
   public synchronized void unpin(Buffer buff) {
      /* Write your code */
   }

   public void printStatus(){
      /* Write your code */
   } 

   public float getHitRatio(){
        /* Write your code */
      return (float)0.00;
    }
   
}
