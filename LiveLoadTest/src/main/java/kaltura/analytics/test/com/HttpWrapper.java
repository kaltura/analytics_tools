package kaltura.analytics.test.com;

import com.ning.http.client.*;
import kaltura.analytics.test.env.SimParams;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class HttpWrapper
{
    private static long _sendCounter = 0;

    private static HttpWrapper instance = null;

    private static AsyncHttpClient _asyncHttpClient = null;

    private DateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");

    protected HttpWrapper()
    {
        _asyncHttpClient = new AsyncHttpClient();
        // Exists only to defeat instantiation.
    }
    public static HttpWrapper getInstance() {
        if( instance == null )
        {
            instance = new HttpWrapper();

        }
        return instance;
    }

    public void close()
    {
//        if ( _asyncHttpClient == null )
//            return;

        _asyncHttpClient.close();
        _asyncHttpClient = null;
    }

    public void send(long time, String partnerId, String entryId, String referrerId, String locationId, String eventIndex, String eventType )
    {
        try
        {
            Date currentDate = new Date(time);

            String checkStr = dateFormat.format(currentDate);

            //_asyncHttpClient.prepareGet("http://pa-live-stats1.kaltura.com/api_v3/index.php")
            _asyncHttpClient.prepareGet(SimParams._nginxURL() )
                    .addHeader("etime", dateFormat.format(currentDate) )
                    .addHeader("eip", locationId)
                    .addQueryParameter("service", "LiveStats")
                    .addQueryParameter("action", "collect")
                    .addQueryParameter("event:partnerId", partnerId)
                    .addQueryParameter("event:entryId", entryId)
                    .addQueryParameter("event:referrer", referrerId)
                    .addQueryParameter("event:eventIndex", eventIndex)
                    .addQueryParameter("event:eventType", eventType)
                    .addQueryParameter("event:bitrate", Integer.toString(SimParams._bitRate() ) )
                    .addQueryParameter("event:bufferTime", Integer.toString(SimParams._bufferTime() ) )
                    .addQueryParameter("event:dummy", "0")
                    .execute(new AsyncCompletionHandler<Response>() {

                        @Override
                        public Response onCompleted(Response response) throws Exception {
                            // Do something with the Response
                            // ...
                            //System.out.println("receive response!"); // Display the string.
                            return response;
                        }

                        @Override
                        public void onThrowable(Throwable t) {
                            System.out.println("failure!"); // Display the string.
                            // Something wrong happened.
                        }
                    });

            //Thread.sleep(2000);


        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
//        finally
//        {
//            _asyncHttpClient.close();
//        }

        _sendCounter++;
    }

    public long sendCounter()
    {
        return  _sendCounter;
    }
}