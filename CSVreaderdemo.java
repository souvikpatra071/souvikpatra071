import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
public class CSVreaderdemo{
   
    private static final String USER_AGENT = "Mozilla/5.0";

	private static final String GET_URL = "https://en.wikipedia.org/wiki/List_of_prime_ministers_of_India";

	private static final String POST_PARAMS = "userName=Souvik";

	public static void main(String[] args) throws IOException {

		URL url = new URL("https://en.wikipedia.org/wiki/List_of_prime_ministers_of_India");

        // Open a connection(?) on the URL(??) and cast the response(???)
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        // Now it's "open", we can set the request method, headers etc.
        connection.setRequestProperty("accept", "application/json");

         // This line makes the request
        InputStream responseStream = connection.getInputStream();

        // Manually converting the response body InputStream to APOD using Jackson
        ObjectMapper mapper = new ObjectMapper();
        APOD apod = mapper.readValue(responseStream, APOD.class);

        // Finally we have the response
        System.out.println(apod.title);

   
    }  
}

