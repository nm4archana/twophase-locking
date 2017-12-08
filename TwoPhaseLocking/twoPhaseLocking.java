// Author : Archana Neelipalayam Masilamani

import java.io.File;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class twoPhaseLocking 
{
	
	Map<Integer, transactionTable> transactionObjList = new HashMap<Integer, transactionTable>();
	Map<String, lockTable> lockTableObjList = new HashMap<String, lockTable>();
	int timeStamp = 0;
	ArrayList<String> commitedList = new ArrayList<String>();
	ArrayList<String> abortedList = new ArrayList<String>();
	
	//Transaction table DS
	class transactionTable
	{
		int tid;
		int ts;
		String transactionState;
		ArrayList<String> itemsHeld;
		ArrayList<String> waitOnBlock;	
	   
		public transactionTable()
		{
			timeStamp++;
	
		}
	}
	
	//LockTable DS
	class lockTable
	{
		String itemName;
		String lockState;
		ArrayList<Integer> lockTid;
		ArrayList<String> waitingTransactions; 
	}
	//Start of operation
	public void startOperation(String opr)
	{
			if(opr.startsWith("b"))
			{
				beginOpr(opr);
			}
			else if(opr.startsWith("r"))
			{
				readOpr(opr);
			}
			else if(opr.startsWith("w"))
			{
				writeOpr(opr);
			}
			else if(opr.startsWith("e"))
			{
				endOpr(opr,0);
			}
	}	
	//Begin Opr
	public void beginOpr(String opr)
	{
		transactionTable transactionObj = new transactionTable();
		ArrayList<String> itemsHeld = new ArrayList<String>();
		ArrayList<String> waitOnBlock = new ArrayList<String>();
		
		transactionObj.tid = Character.getNumericValue(opr.toCharArray()[1]);
		transactionObj.ts = timeStamp;
		transactionObj.itemsHeld = itemsHeld;
		transactionObj.waitOnBlock = waitOnBlock;
		transactionObj.transactionState = "active";	
		transactionObjList.put(transactionObj.tid, transactionObj);	
		String op =  opr.replaceAll(";$", "");
		System.out.println(op+" - Begin of Transaction T"+transactionObj.tid+" with Timestamp "+transactionObj.ts);
	}
	
	
	//Read Opr
	public void readOpr(String opr)
	{
		int currTid  =  Character.getNumericValue(opr.toCharArray()[1]);
		transactionTable transactionObj = transactionObjList.get(currTid);
		
		if(transactionObj.transactionState.equalsIgnoreCase("abort"))
		{
			//Do nothing
			String op =  opr.replaceAll(";$", "");
			System.out.println(op+" - Operation "+op+" is ignored since Transaction T"+currTid+" is already aborted");
		}
		else if(transactionObj.transactionState.equalsIgnoreCase("blocked"))
		{
			//If the transaction is already blocked, then add the operation to the 
			//waitList of the transaction
			ArrayList<String> waitList = transactionObj.waitOnBlock;
			waitList.add(opr);
			transactionObj.waitOnBlock = waitList;
		}
		else if(transactionObj.transactionState.equalsIgnoreCase("active"))
		{
			//Check if the item is already locked
			Matcher match = Pattern.compile("\\((.*?)\\)").matcher(opr);
			String dataItem = "";
			while(match.find()) {
			     dataItem = match.group(1);
			}
			
			//If the item is not locked
			if(!lockTableObjList.containsKey(dataItem))
			{
				//Creating a lock object
				lockTable lockObj = new lockTable();
				lockObj.itemName = dataItem;
				lockObj.lockState = "RL";
				
				int lTid = Character.getNumericValue(opr.toCharArray()[1]);
				ArrayList<Integer> lockTid = new ArrayList<Integer>();
				lockTid.add(lTid);
				
				lockObj.lockTid = lockTid;
				
				ArrayList<String> waitingTransactions = new ArrayList<String>();
				lockObj.waitingTransactions = waitingTransactions;	
				
				//Adding lock object to the map list
				lockTableObjList.put(dataItem, lockObj);
				String op =  opr.replaceAll(";$", "");
				System.out.println(op+" - Transaction T"+transactionObj.tid+" locks data item "+dataItem+" in "
						+ "Read Lock mode and reads "+dataItem);
				//Add the data item to items held list of that transaction object
				ArrayList<String> itemsHeldList = transactionObj.itemsHeld;
				itemsHeldList.add(dataItem);
				
				transactionObj.itemsHeld = itemsHeldList;
				
				//Add the transaction object back to the list
				transactionObjList.put(transactionObj.tid, transactionObj);
								
			}
			//If the item is already locked
			else if(lockTableObjList.containsKey(dataItem))
			{
				lockTable lockObj	=  lockTableObjList.get(dataItem);
				//If the item is locked by another read lock
				if(lockObj.lockState.equals("RL"))
				{
					//Add the data item to items held list of that transaction object
					ArrayList<String> itemsHeldList = transactionObj.itemsHeld;
					itemsHeldList.add(dataItem);
					
					transactionObj.itemsHeld = itemsHeldList;
					
					//Add the transaction object back to the list
					transactionObjList.put(transactionObj.tid, transactionObj);
					
					//Add the transaction id to lock id list of that lock table
					ArrayList<Integer> lockIdList = lockObj.lockTid;
					lockIdList.add(currTid);
					
					lockObj.lockTid = lockIdList;
					
					//Add the lock object back to Lock Table
					lockTableObjList.put(dataItem, lockObj);
					String op =  opr.replaceAll(";$", "");
					System.out.println(op+" - Data item "+dataItem+" is already locked by another transaction in Read Lock mode. Transaction T"+transactionObj.tid+" locks data item "+dataItem+" in "
							+ "Read Lock mode and reads "+dataItem);
					
				}						
				//if the item is locked by another write lock - check for deadlock
				else if(lockObj.lockState.equals("WL"))
				{
					//Check for deadlock
					int flag = 0;
					for(int h=0;h<lockObj.lockTid.size();h++)
					{
						if(transactionObj.ts<lockObj.lockTid.get(h))
						{
							flag = 1;
							break;
						}
					}		
					//If requesting transactions TS < TS of locked object, the requesting transaction waits
					if(flag==1)
					{
						//Add the data item to wait on block list of that transaction object
						ArrayList<String> waitOnBlockList = transactionObj.waitOnBlock;
						waitOnBlockList.add(opr);
						
						transactionObj.waitOnBlock = waitOnBlockList;
						transactionObj.transactionState = "blocked";
						
						//Add the transaction object back to the list
						transactionObjList.put(transactionObj.tid, transactionObj);
						
						
						//Add the operation to wait transactions list of that lock table
						ArrayList<String> waitList = lockObj.waitingTransactions;
						waitList.add(opr);
						
						lockObj.waitingTransactions = waitList;
						
						//Add the lock object back to Lock Table
						lockTableObjList.put(dataItem, lockObj);
						String op =  opr.replaceAll(";$", "");
						System.out.println(op+" - Transaction T"+currTid+" is blocked. It waits for data item "+dataItem);
										
					}
					//If requesting transactions TS > TS of locked object, the requesting transaction aborts
					else
					{
						//Change the transaction Object State to abort
						transactionObj.transactionState="abort";
						transactionObjList.put(transactionObj.tid, transactionObj);
						
						abortOpr(opr);
					}
				}
			}
		}
	}
	
	//Write Opr
	public void writeOpr(String opr)
	{
		int currTid  =  Character.getNumericValue(opr.toCharArray()[1]);
		transactionTable transactionObj = transactionObjList.get(currTid);
		
		if(transactionObj.transactionState.equalsIgnoreCase("abort"))
		{
			//Do nothing
			String op =  opr.replaceAll(";$", "");
			System.out.println(op+" - Operation "+op+" is ignored since Transaction T"+currTid+" is already aborted");
		}
		else if(transactionObj.transactionState.equalsIgnoreCase("blocked"))
		{
			//If the transaction is already blocked, then add the operation to the 
			//waitList of the transaction
			ArrayList<String> waitList = transactionObj.waitOnBlock;
			waitList.add(opr);
			transactionObj.waitOnBlock = waitList;
		}
		else if(transactionObj.transactionState.equalsIgnoreCase("active"))
		{
			//Check if the item is already locked
			Matcher match = Pattern.compile("\\((.*?)\\)").matcher(opr);
			String dataItem = "";
			while(match.find()) {
			     dataItem = match.group(1);
			}
			
			//If the item is not locked
			if(!lockTableObjList.containsKey(dataItem))
			{
				//Creating a lock object
				lockTable lockObj = new lockTable();
				lockObj.itemName = dataItem;
				lockObj.lockState = "WL";
				
				int lTid = Character.getNumericValue(opr.toCharArray()[1]);
				ArrayList<Integer> lockTid = new ArrayList<Integer>();
				lockTid.add(lTid);
				
				lockObj.lockTid = lockTid;
				
				ArrayList<String> waitingTransactions = new ArrayList<String>();
				lockObj.waitingTransactions = waitingTransactions;	
				
				//Adding lock object to the map list
				lockTableObjList.put(dataItem, lockObj);
				String op =  opr.replaceAll(";$", "");
				System.out.println(op+" - Transaction T"+transactionObj.tid+" locks data item "+dataItem+" in "
						+ "Write Lock mode and writes "+dataItem);
				
				//Add the data item to items held list of that transaction object
				ArrayList<String> itemsHeldList = transactionObj.itemsHeld;
				itemsHeldList.add(dataItem);
				
				transactionObj.itemsHeld = itemsHeldList;
				
				//Add the transaction object back to the list
				transactionObjList.put(transactionObj.tid, transactionObj);
								
			}
			//If the item is already locked
			else if(lockTableObjList.containsKey(dataItem))
			{
				lockTable lockObj	=  lockTableObjList.get(dataItem);
						
					//Check for transaction ID for upgrade operation
					if(lockObj.lockTid.size()==1&&lockObj.lockTid.get(0)==currTid)
					{
						lockObj.lockState = "WL";
						lockTableObjList.put(dataItem, lockObj);
						String op =  opr.replaceAll(";$", "");
						System.out.println(op+" - Read Lock of Transaction T"+transactionObj.tid+" is "
								+ "upgraded to Write Lock on data item "+dataItem);
					}
					
					else{
					//If requesting transactions TS < TS of locked object, the requesting transaction waits
						//System.out.println(lockObj.lockTid.get(0));
						//Ssystem.out.println(transactionObj.ts);
						int flag = 0;
						for(int h=0;h<lockObj.lockTid.size();h++)
						{
							if(transactionObj.ts<lockObj.lockTid.get(h))
							{
								flag = 1;
								break;
							}
						}						
					if(flag == 1)
					{
						//Add the data item to wait on block list of that transaction object
						ArrayList<String> waitOnBlockList = transactionObj.waitOnBlock;
						waitOnBlockList.add(opr);
						
						transactionObj.waitOnBlock = waitOnBlockList;
						transactionObj.transactionState = "blocked";
						
						//Add the transaction object back to the list
						transactionObjList.put(transactionObj.tid, transactionObj);
						
						
						//Add the operation to wait transactions list of that lock table
						ArrayList<String> waitList = lockObj.waitingTransactions;
						waitList.add(opr);
						
						lockObj.waitingTransactions = waitList;
						
						//Add the lock object back to Lock Table
						lockTableObjList.put(dataItem, lockObj);
						String op =  opr.replaceAll(";$", "");
						System.out.println(op+" - Transaction T"+currTid+" is blocked. It waits for data item "+dataItem);
										
					}
					//If requesting transactions TS > TS of locked object, the requesting transaction aborts
					else
					{
						//Change the transaction Object State to abort
						transactionObj.transactionState="abort";
						transactionObjList.put(transactionObj.tid, transactionObj);
						
						abortOpr(opr);
					}
				}
			
		}}
	}

	//End Opr
	public void endOpr(String opr, int flag)
	{
		Matcher match = Pattern.compile("\\((.*?)\\)").matcher(opr);
		String dataItem = "";
		while(match.find()) {
		     dataItem = match.group(1);
		}

		int currTid  =  Character.getNumericValue(opr.toCharArray()[1]);
		transactionTable transactionObj = transactionObjList.get(currTid);
		//System.out.println(currTid+","+transactionObj.transactionState);
		if(transactionObj.transactionState.equalsIgnoreCase("abort")&&flag==0)
		{	
			//Do nothing
			String op =  opr.replaceAll(";$", "");
			System.out.println(op+" - Operation "+opr+" is ignored since Transaction T"+currTid+" is already aborted");
		}
		else if(transactionObj.transactionState.equalsIgnoreCase("blocked"))
		{
			//If the transaction is already blocked, then add the operation to the 
			//waitList of the transaction
			ArrayList<String> waitList = transactionObj.waitOnBlock;
			waitList.add(opr);
			transactionObj.waitOnBlock = waitList;
			String op =  opr.replaceAll(";$", "");
			System.out.println(op+" - Transaction T"+currTid+" is already blocked. So add operation \""+ opr+"\" to wait list");
		}
		else
		{
			if(flag == 0 || !transactionObj.transactionState.equalsIgnoreCase("abort"))
			{
			String op =  opr.replaceAll(";$", "");
			System.out.println(op+" - Transaction T"+currTid+" is committed");
			commitedList.add("T"+currTid);
		 	transactionObj.transactionState = "committed";
		 	}
 
		 	ArrayList<String> itemsHeld = transactionObj.itemsHeld;
		 	
		 	for(int i=0;i<itemsHeld.size();i++)
		 	{
		 		
		 		lockTable lockTableObj = lockTableObjList.get(itemsHeld.get(i));
		 		if(lockTableObj!=null){
		 		ArrayList<Integer> lockTid = lockTableObj.lockTid;
		 		
		 		for(int k=0;k<lockTid.size();k++)
		 		{
		 			if(lockTid.get(k)==currTid)
		 			{
		 				lockTid.remove(k);
		 			}
		 		}
		 		
		 		
		 		
		 		ArrayList<String> waitList = lockTableObj.waitingTransactions;
		 		
		 		lockTableObj.lockTid = lockTid;
		 		if(lockTid.size()==0&&waitList.size()==0)
		 		{
		 			lockTableObjList.remove(itemsHeld.get(i));
		 		}
		 		else
		 		lockTableObjList.put(itemsHeld.get(i), lockTableObj);
		 		
		 		if(waitList.size()>0){
		 			ArrayList<String> changedList = new ArrayList<String>();	
		 			
		 			for(int w=0;w<waitList.size();w++)
		 			{
		 				int wTid = Character.getNumericValue(waitList.get(w).toCharArray()[1]);
		 				Matcher match1 = Pattern.compile("\\((.*?)\\)").matcher(waitList.get(w));
		 				String dataItem1 = "";
		 				while(match1.find()) {
		 				     dataItem1 = match1.group(1);
		 				}
		 						
		 				if(lockTableObj.lockTid.size()==0)
		 				{
		 					lockTableObj.lockTid.add(wTid);
		 					
		 					if(waitList.get(w).startsWith("w"))
		 					{
		 					lockTableObj.lockState="WL";
		 					String op =  opr.replaceAll(";$", "");
		 					System.out.println(op+" - Transaction T"+wTid+" is released from wait list and write locks data item "+dataItem1);
		 					}
		 					else if(waitList.get(w).startsWith("r"))
		 					{
		 					lockTableObj.lockState="RL";
		 					String op =  waitList.get(w).replaceAll(";$", "");
		 					System.out.println(op+" - Transaction T"+wTid+" is released from wait list and read locks data item "+dataItem1);		 				 	
		 					}
		 					
		 					changedList.add(waitList.get(w));
		 					waitList.remove(w);
		 				}
		 				else if(lockTableObj.lockTid.size()>=1&&lockTableObj.lockState=="RL"&&waitList.get(w).startsWith("r"))
		 				{
		 					//Shared read lock
		 					lockTableObj.lockTid.add(wTid);
		 					String op =  waitList.get(w).replaceAll(";$", "");
		 					System.out.println(op+" - Transaction T"+wTid+" is released from wait list and read locks data item "+dataItem1);		 				 	

		 				}
		 				else if(lockTableObj.lockTid.size()==1&&lockTableObj.lockState=="RL"&&wTid==lockTableObj.lockTid.get(0)&&waitList.get(w).startsWith("w"))
		 				{
		 					//Upgrade
		 					lockTableObj.lockState="WL";
		 					changedList.add(waitList.get(w));
		 					String op =  waitList.get(w).replaceAll(";$", "");
		 					waitList.remove(w);
		 					
		 					System.out.println(op+" - Transaction T"+wTid+" is released from wait list and upgrades read lock to write lock on data item "+dataItem1);		 				 	

		 				}
		 				else
		 				{
		 					
		 				}
		 				lockTableObj.waitingTransactions=waitList;
		 				lockTableObjList.put(dataItem1, lockTableObj);
		 				
		 			}
		 			
		 			//Make necessary changes in transaction table based on changed list
		 			for(int c=0;c<changedList.size();c++)
		 			{
		 				int cTid = Character.getNumericValue(changedList.get(c).toCharArray()[1]);
		 				Matcher match1 = Pattern.compile("\\((.*?)\\)").matcher(changedList.get(c));
		 				String dataItem1 = "";
		 				while(match1.find()) {
		 				     dataItem1 = match1.group(1);
		 				}
		 				
		 				transactionTable trTableObj = transactionObjList.get(cTid);
		 				ArrayList<String> itemHeld = trTableObj.itemsHeld;
		 				itemHeld.add(dataItem1);
		 				ArrayList<String> waitOnBlock = trTableObj.waitOnBlock;
		 				
		 				//Remove the operation from Wait on Hold list of the transaction from transaction table
		 				for(int m=0;m<waitOnBlock.size();m++)
		 				{
		 					if(waitOnBlock.get(m)==changedList.get(c))
		 					{
		 						waitOnBlock.remove(c);
		 					}
		 				}
		 				
		 				if(waitOnBlock.size()==0)
		 				{
		 					trTableObj.transactionState="active";
		 				}
		 				else if(waitOnBlock.size()==1&&waitOnBlock.get(0).startsWith("e"))
		 				{
		 					trTableObj.transactionState="active";
		 					transactionObjList.put(cTid, trTableObj);
		 					
		 					endOpr(waitOnBlock.get(0),0);
		 				}
		 				trTableObj.waitOnBlock = waitOnBlock;
		 				trTableObj.itemsHeld = itemHeld;
		 				transactionObjList.put(cTid, trTableObj);
		 			}
		 		}
		 		else
		 		{
		 			lockTableObjList.remove(dataItem);
		 		}		 				 		
		 		
		 	}
		 		
		}
		 	ArrayList<String> list = new ArrayList<String>();
		 	transactionObj.itemsHeld = list;
		 	transactionObjList.put(currTid, transactionObj);
		}
	}
		
	//Abort Opr
	public void abortOpr(String opr)
	{
		
		int currTid  =  Character.getNumericValue(opr.toCharArray()[1]);
		String op =  opr.replaceAll(";$", "");
		System.out.println(op+" - Transaction T"+currTid+" is aborted");
		abortedList.add("T"+currTid);
		int aflag = 1;
		endOpr(opr,aflag);
		aflag = 0;
	}
			
	
	
	@SuppressWarnings("resource")
	public static void main(String args[])
	{
		//Scan Input File
		twoPhaseLocking locking = new twoPhaseLocking();
		
		
		try {
            System.out.print("Enter the path of the input file: ");
            //    /Users/archana/Desktop/input.txt
            Scanner input = new Scanner(System.in);

            File file = new File(input.nextLine());

            input = new Scanner(file);

            while (input.hasNextLine()) {
                String opr = input.nextLine();
                locking.startOperation(opr);
            }
            
            input.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    
		//Print the committed and aborted transactions
		System.out.println(" ");
		if(locking.commitedList.size()==0)
		{
			System.out.println("Committed Transactions: None");
		}
		else{
		System.out.print("Committed Transactions: ");
		for(int i=0;i<locking.commitedList.size();i++)
		{
			System.out.print(locking.commitedList.get(i));
			if(i+1!=locking.commitedList.size())
			{
				System.out.print(" , ");
			}
		}
		}
		
		//Print aborted transactions
		System.out.println(" ");
		if(locking.abortedList.size()==0)
		{
			System.out.println("Aborted Transactions: None");
		}
		else{
		System.out.print("Aborted Transactions: ");
		for(int i=0;i<locking.abortedList.size();i++)
		{
			System.out.print(locking.abortedList.get(i));
			if(i+1!=locking.abortedList.size())
			{
				System.out.print(" , ");
			}
		}
		}
		
	}
}
