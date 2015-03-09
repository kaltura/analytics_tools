
import com.ning.http.client.*;
import kaltura.analytics.test.core.EventGenerator;

import java.util.concurrent.Future;

class SimpleClient {
//    public static void main(String[] args)
//    {
//        int nSessions = 1;
//        if ( args.length > 0 )
//
//        EventGenerator.run();
//    }

    public static void dummy()
    {

        AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
        try
        {
            //EventGenerator.run();
            /////////////////////////// 1.
//            AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
//            Future<Integer> f = asyncHttpClient.prepareGet("http://il-bigdata-3.dev.kaltura.com/").execute(
//                    new AsyncCompletionHandler<Integer>(){
//
//                        @Override
//                        public Integer onCompleted(Response response) throws Exception{
//                            // Do something with the Response
//                            System.out.println("receive response!"); // Display the string.
//                            return response.getStatusCode();
//                        }
//
//                        @Override
//                        public void onThrowable(Throwable t){
//                            // Something wrong happened.
//                        }
//                    });
//
//            int statusCode = f.get();
            /////////////////////////// 1.

            /////////////////////////// 2.
//            AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
//            Future<Response> f = asyncHttpClient.prepareGet("http://il-bigdata-3.dev.kaltura.com/dummypage/").execute();
//            Response r = f.get();
            /////////////////////////// 2.

            /////////////////////////// 3.



            asyncHttpClient.prepareGet("http://il-bigdata-3.dev.kaltura.com/")
                    .addHeader("Content-Time", "SpecialDummyContent")
                    .addHeader("Content-IP", "SpecialDummyContent")
                    .addQueryParameter("param1", "value1")
                    .addQueryParameter("param2", "value2")
                    .execute(new AsyncCompletionHandler<Response>() {

                        @Override
                        public Response onCompleted(Response response) throws Exception {
                            // Do something with the Response
                            // ...
                            System.out.println("receive response!"); // Display the string.
                            return response;
                        }

                        @Override
                        public void onThrowable(Throwable t) {
                            System.out.println("failure!"); // Display the string.
                            // Something wrong happened.
                        }
                    });
            /////////////////////////// 3.

            /////////////////////////// 3.5
//            final AsyncHttpClient.BoundRequestBuilder requestBuilder = asyncHttpClient.prepareGet("http://il-bigdata-3.dev.kaltura.com//rest/1.0/event")
//                    .addHeader("Content-Type", "DummyContent")
//                    .setBody("Body-abcd");
//            //if (name != null) {
//                requestBuilder.addQueryParameter("name", "DummyName");
//            //}
//            //if (dateTime != null) {
//                requestBuilder.addQueryParameter("date", "21/3/2014-08:20");
//            //}
//
//            final Response response = requestBuilder.execute().get();
//            //Assert.assertEquals(response.getStatusCode(), 202);

            /////////////////////////// 3.5

            /////////////////////////// 4.
//            AsyncHttpClient c = new AsyncHttpClient();
//            Future f = c.prepareGet("http://www.ning.com/").execute(new AsyncCompletionHandler() {
//
//                @Override
//                public Response onCompleted(Response response) throws Exception {
//                    // Do something
//                    return response;
//                }
//
//                @Override
//                public void onThrowable(Throwable t) {
//                }
//            });
//            Response response = f.get();
            /////////////////////////// 4.
            Thread.sleep(2000);


        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            asyncHttpClient.close();
        }




        System.out.println("End"); // Display the string.
        //System.exit(0);
    }
}

