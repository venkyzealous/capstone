
class ZipCodeData {
	double lat;
	double lon;
	String city;
	String state_name;
	String postId;

	public ZipCodeData(double lat, double lon, String city, String state_name, String postId) {
		this.lat = lat;
		this.lon = lon;
		this.city = city;
		this.state_name = state_name;
		this.postId = postId;
	}
}