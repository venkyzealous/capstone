import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TransactionData implements Serializable {

	public String getCard_id() {
		return card_id;
	}

	public void setCard_id(String card_id) {
		this.card_id = card_id;
	}

	public String getMember_id() {
		return member_id;
	}

	public void setMember_id(String member_id) {
		this.member_id = member_id;
	}

	public double getAmount() {
		return amount;
	}

	public void setAmount(double amount) {
		this.amount = amount;
	}

	public String getPostcode() {
		return postcode;
	}

	public void setPostcode(String postcode) {
		this.postcode = postcode;
	}

	public String getPos_id() {
		return pos_id;
	}

	public void setPos_id(String pos_id) {
		this.pos_id = pos_id;
	}

	public String getTransaction_dt() {
		return transaction_dt;
	}

	public void setTransaction_dt(String transaction_dt) {
		this.transaction_dt = transaction_dt;
	}

	public double getUcl() {
		return ucl;
	}

	public void setUcl(double ucl) {
		this.ucl = ucl;
	}

	public int getScore() {
		return score;
	}

	public void setScore(int score) {
		this.score = score;
	}

	public String getLast_postcode() {
		return last_postcode;
	}

	public void setLast_postcode(String last_postcode) {
		this.last_postcode = last_postcode;
	}

	public String getLast_transaction_dt() {
		return last_transaction_dt;
	}

	public void setLast_transaction_dt(String last_transaction_dt) {
		this.last_transaction_dt = last_transaction_dt;
	}

	public boolean isDistanceValid() {

		try {
			DistanceUtility utility = new DistanceUtility();
			double distance_km = utility.getDistanceViaZipCode(this.postcode, this.last_postcode);
			long time_secs;
			SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
			Date cur_txn_date = formatter.parse(this.transaction_dt);
			Date last_txn_date = new Date((long) (Long.parseLong(this.last_transaction_dt) * 1000));
			time_secs = (Math.abs(cur_txn_date.getTime() - last_txn_date.getTime())) / 1000;
			double speed_kmsec = distance_km / time_secs;
			
			if(speed_kmsec > 4) return false;
			else return true;

		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return false;
	}

	private String card_id;
	private String member_id;
	private double amount;
	private String postcode;
	private String pos_id;
	private String transaction_dt;
	private double ucl;
	private int score;
	private String last_postcode;
	private String last_transaction_dt;

}
