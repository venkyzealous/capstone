import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
 * HBase DAO class that provides different operational handlers.
 * 
 */
public class HBaseDAO {
	
	 private static final String LOOKUP_TABLE_NAME = "lookup_card_member";
	 private static final String TXN_TABLE_NAME = "card_transactions";
	 private static final String CF_CARD = "card_details";
	 private static final String CF_TXN = "transaction_details";
	 private static final String CF_LAST_TXN = "last_transaction_details";
	 private static final String C_SCORE = "score";
	 private static final String C_UCL = "ucl";
	 private static final String C_POSTCODE = "postcode";
	 private static final String C_TXN_DT = "transaction_dt";
	 private static final Integer MAX_SCORE = 200;
	 
	  
	/**
	 *
	 * 
	 * 
	 * @param transactionData
	 * 
	 * @return get member's score from look up HBase table.
	 * 
	 * @throws IOException
	 * 
	 */

	public static Integer getScore(TransactionData transactionData) throws IOException {
		Table table = null;

		try {
			Get g = new Get(Bytes.toBytes(transactionData.getCard_id()));
			table = HBaseConnection.getTable(LOOKUP_TABLE_NAME);
			Result result = table.get(g);
			byte[] value = result.getValue(Bytes.toBytes(CF_LAST_TXN), Bytes.toBytes(C_SCORE));
			if (value != null) {
				return Integer.parseInt(Bytes.toString(value));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (table != null)
					table.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	public static void getCardData(TransactionData transactionData) throws IOException {
		Table table = null;

		try {
			Get g = new Get(Bytes.toBytes(transactionData.getCard_id()));
			table = HBaseConnection.getTable(LOOKUP_TABLE_NAME);
			Result result = table.get(g);
			
			byte[] value = result.getValue(Bytes.toBytes(CF_LAST_TXN), Bytes.toBytes(C_SCORE));
			if (value != null) {
				transactionData.setScore(Integer.parseInt(Bytes.toString(value)));
			}
			
			byte[] ucl = result.getValue(Bytes.toBytes(CF_LAST_TXN), Bytes.toBytes(C_UCL));
			if (value != null) {
				transactionData.setUcl(Double.parseDouble(Bytes.toString(ucl)));
			}
			
			byte[] postcode = result.getValue(Bytes.toBytes(CF_LAST_TXN), Bytes.toBytes(C_POSTCODE));
			if (value != null) {
				transactionData.setLast_postcode(Bytes.toString(postcode));
			}
			
			byte[] transaction_dt = result.getValue(Bytes.toBytes(CF_LAST_TXN), Bytes.toBytes(C_TXN_DT));
			if (value != null) {
				transactionData.setLast_transaction_dt(Bytes.toString(transaction_dt));
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (table != null)
					table.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	
	public static boolean isGenuine(TransactionData txn){
		
		try {
			boolean isGenuine=true;
			HBaseDAO.getCardData(txn);
			
			if(txn.getAmount() > txn.getUcl())
				isGenuine = false;
			if(txn.getScore() < MAX_SCORE)
				isGenuine = false;
			if(!txn.isDistanceValid())
				isGenuine = false;
			/*System.out.println("score ="+ txn.getScore());
			System.out.println("ucl ="+ txn.getUcl());
			System.out.println("postcode ="+ txn.getLast_postcode());
			System.out.println("transaction_dt ="+ txn.getTransaction_dt());*/
			
			return isGenuine;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	public static void printTxn(TransactionData txn, boolean isGenuine)  {
		//print classification result to console
		
		ObjectMapper mapper = new ObjectMapper();
		try {
			System.out.println(mapper.writeValueAsString(txn));
			if(isGenuine)
			System.out.println("GENUINE");
			else System.out.println("FRAUD");
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void save(TransactionData txn, boolean isGenuine)  {
		
		try {
			Table table = HBaseConnection.getTable(TXN_TABLE_NAME);
			SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
			Date cur_txn_date = formatter.parse(txn.getTransaction_dt());
			
			
			String key = txn.getCard_id() + "~" + cur_txn_date.getTime(); 
			Put txn_record = new Put(Bytes.toBytes(key));
			
			txn_record.addColumn(Bytes.toBytes("card_details"), Bytes.toBytes("card_id"), 
					Bytes.toBytes(txn.getCard_id()));
			txn_record.addColumn(Bytes.toBytes("card_details"), Bytes.toBytes("member_id"), 
					Bytes.toBytes(txn.getMember_id()));
			txn_record.addColumn(Bytes.toBytes("transaction_details"), Bytes.toBytes("amount"), 
					Bytes.toBytes(txn.getAmount()));
			txn_record.addColumn(Bytes.toBytes("transaction_details"), Bytes.toBytes("postcode"), 
					Bytes.toBytes(txn.getPostcode()));
			txn_record.addColumn(Bytes.toBytes("transaction_details"), Bytes.toBytes("pos_id"), 
					Bytes.toBytes(txn.getPos_id()));
			txn_record.addColumn(Bytes.toBytes("transaction_details"), Bytes.toBytes("transaction_dt"), 
					Bytes.toBytes(((Long)cur_txn_date.getTime()).toString()));
			txn_record.addColumn(Bytes.toBytes("transaction_details"), Bytes.toBytes("status"), 
					Bytes.toBytes((isGenuine)?"GENUINE":"FRAUD"));
			table.put(txn_record);
			
			Table lookup_table = HBaseConnection.getTable(LOOKUP_TABLE_NAME);
			Put lookup_record = new Put(Bytes.toBytes(txn.getCard_id()));
			
			lookup_record.addColumn(Bytes.toBytes("last_transaction_details"), Bytes.toBytes("postcode"), 
					Bytes.toBytes(txn.getPostcode()));
			lookup_record.addColumn(Bytes.toBytes("last_transaction_details"), Bytes.toBytes("transaction_dt"), 
					Bytes.toBytes(((Long)cur_txn_date.getTime()).toString()));
			
			lookup_table.put(lookup_record);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	
	}
}
