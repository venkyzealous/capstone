import java.util.ArrayList;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TransactionProcessor {

	private JavaDStream<String> ds;
	public static ArrayList<TransactionData> data = new ArrayList<TransactionData>(); 

	public TransactionProcessor(JavaDStream<String> ds) {
		this.ds = ds;
	}

	public void process() {

		JavaDStream<TransactionData> result = ds.map(x -> {
			ObjectMapper mapper = new ObjectMapper();
			/*System.out.println("input transaction: -");
			System.out.println(x);*/
			if(x == null) return null;
			try {
				TransactionData txn = mapper.readValue(x, TransactionData.class);
				return txn;
			} catch (JsonParseException ex) {
				System.out.println("could not parse input");
				return null;
			}
			catch (JsonMappingException ex) {
				System.out.println("could not map input");
				return null;
			}

		});

		result.foreachRDD(new VoidFunction<JavaRDD<TransactionData>>() {
			private static final long serialVersionUID = 2L;

			@Override
			public void call(JavaRDD<TransactionData> rdd) {
				rdd.foreach(txn -> {

					if (txn != null) {
						TransactionProcessor.data.add(txn);
						if (HBaseDAO.isGenuine(txn)) {
							HBaseDAO.printTxn(txn, true);
							HBaseDAO.save(txn, true);
						} else {
							HBaseDAO.printTxn(txn, false);
							HBaseDAO.save(txn, false);
						}
						
					}
				});
			}
		});
		
		//save data to csv file
		

	}

}
