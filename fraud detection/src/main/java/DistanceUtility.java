import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

class DistanceUtility {

	HashMap<String, ZipCodeData> zipCodesMap = new HashMap<String, ZipCodeData>();

	/**
	 * Initialize zip codes using given file
	 * @throws IOException 
	 * @throws NumberFormatException 
	 */
	public DistanceUtility() throws NumberFormatException, IOException {

		BufferedReader br = new BufferedReader(new FileReader("zipCodePosId.csv"));

		String line = null;

		while ((line = br.readLine()) != null) {
			String str[] = line.split(",");

			String zipCode = str[0];

			double lat = Double.parseDouble(str[1]);
			double lon = Double.parseDouble(str[2]);
			;
			String city = str[3];
			String state_name = str[4];
			String postId = str[5];

			ZipCodeData zipCodeData = new ZipCodeData(lat, lon, city, state_name, postId);

			zipCodesMap.put(zipCode, zipCodeData);
		}
		
		br.close();
	}

	/**
	 *
	 * @param zipcode1
	 *            - zip code of previous transaction
	 * @param zipcode2
	 *            - zip code of current transaction
	 * @return distance between two zip codes
	 */
	public double getDistanceViaZipCode(String zipcode1, String zipcode2) {
		ZipCodeData z1 = zipCodesMap.get(zipcode1);
		ZipCodeData z2 = zipCodesMap.get(zipcode2);
		return distance(z1.lat, z1.lon, z2.lat, z2.lon);
	}

	private double distance(double lat1, double lon1, double lat2, double lon2) {
		double theta = lon1 - lon2;
		double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2))
				+ Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
		dist = Math.acos(dist);
		dist = rad2deg(dist);
		dist = dist * 60 * 1.1515;
		dist = dist * 1.609344;

		return dist;
	}

	private double rad2deg(double rad) {
		return rad * 180.0 / Math.PI;
	}

	private double deg2rad(double deg) {
		return deg * Math.PI / 180.0;
	}

}