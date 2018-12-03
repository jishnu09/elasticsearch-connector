/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.elasticsearch.connector;

/**
 *
 * @author SwarnenduS
 */
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.InetAddress;
import org.apache.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.logging.ESLoggerFactory;
//import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.netty4.Netty4Utils;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;


public class ElasticsearchConnector {

    //private static Logger log = LoggerFactory.getLogger(ElasticsearchConnector.class);
    private static Logger log = LogManager.getLogger(ElasticsearchConnector.class);

    public static void main(String args[]) throws IOException {
        //log.setLevel(Level.DEBUG);
        System.out.println("swarnendu 1");
        ESLoggerFactory.getLogger(ElasticsearchConnector.class).catching(org.apache.logging.log4j.Level.DEBUG, (new Throwable()));
        
        
        //Log4jESLoggerFactory.getLogger("discovery").setLevel("DEBUG");
        /*TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
		        .addTransportAddress(new TransportAddress(InetAddress.getByName("host1"), 9300))
		        .addTransportAddress(new TransportAddress(InetAddress.getByName("host2"), 9300));

		// on shutdown

		client.close();*/
 /*Settings settings = Settings.builder()
		        .put("cluster.name", "myClusterName").build();
		TransportClient client = new PreBuiltTransportClient(settings);
		
         */
 /*Settings settings = Settings.builder()
				.put("cluster.name", "my-application").build();
		
		PreBuiltTransportClient client = new PreBuiltTransportClient(settings);
		client.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
		
		MatchAllQueryBuilder mqb= QueryBuilders.matchAllQuery();
		SearchRequestBuilder sb=client.prepareSearch("supstore").setFrom(0) .setSize(3).setQuery(mqb) ;
		
		SearchResponse searchResponse = sb.execute().actionGet();
		
		System.out.println("searchResponse "+searchResponse);
		
		JSONObject job=new JSONObject(searchResponse);
		
		System.out.println("searchResponse.getHits() "+searchResponse.getHits());
		
		System.out.println("job  "+job);
		
		JSONObject hits1=job.getJSONObject("hits");
		
		System.out.println("hits1"+hits1);

		JSONArray hits2=hits1.getJSONArray("hits");
		
		System.out.println("hits2------------------"+hits2);
         */
 /*IndexResponse response = client.prepareIndex("supstore", "supstore2", "")
                 .setSource("", "")
                 .get();
		
		System.out.println("response "+response);
         */

 /*SearchHit[] results = searchResponse.getHits().getHits();
		System.out.println("results "+results);
		
		
		
		SearchHits searchHits = searchResponse.getHits();
		System.out.println("searchHits "+searchHits);
		System.out.println("Total Hits is " + searchHits.getTotalHits());*/
 /*Map<String, Object> result = null;
		List<Map<String, Object>> resultList =null;
		resultList = new ArrayList<Map<String, Object>>();
		for (SearchHit hit : results) {
			System.out.println(hit.getScore());
			result = hit.getSource();
			
			resultList.add(result);
		    System.out.println(result);
		}
		System.out.println("-----------------------------------------------------------------------------");
		System.out.println(resultList);*/
        org.apache.log4j.Logger.getLogger("client.transport").setLevel(Level.TRACE);
        Client client = null;

        client = esconn();
        //AdminClient ad = client.admin();
log.debug(ESLoggerFactory.getLogger(Netty4Utils.class).getLevel().name());
        mapping(client);
        bulkinsert(client);
//		client.close();
        /*JdbcDataSource dataSource = new JdbcDataSource();
		String address = "jdbc:es://" + "localhost:92000";     
		dataSource.setUrl(address);
		
		Properties connectionProperties = connectionProperties(); 
		dataSource.setProperties(connectionProperties);
		Connection connection = dataSource.getConnection();
		
         */
        System.out.println("end");
    }

    public static Client esconn() {
        //	PreBuiltTransportClient client=null;
        TransportClient client = null;
        try {
            System.out.println("in connection");
            /*		Settings settings = Settings.builder().put("cluster.name", "my-application")
		.put("xpack.security.user", "transport_client_user:x-pack-test-password").build();

		client = new PreBuiltTransportClient(settings);
		//client.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
		client.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
		

             */

            client = new PreBuiltXPackTransportClient(Settings.builder()
                    //.put("cluster.name", "swarnendu")
                    .put("cluster.name", "mcube-es-cluster")
                    //.put("xpack.security.user", "elastic:changeme")
                    .put("xpack.security.user", "elastic:pleasechangeme")
                    .put("xpack.security.transport.ssl.enabled", "true")
                    .put("xpack.ssl.certificate_authorities", "C:/Users/swarnendus/Downloads/ca/ca/ca.crt")
//                    .put("xpack.ssl.key", "C:/Users/swarnendus/Downloads/ca/ca/ca.key")
//                    .put("xpack.ssl.certificate", "C:/Users/swarnendus/Downloads/ca/ca/ca.crt")
                    .put("logger._root", "DEBUG")
                    .put("logger.level", "DEBUG")
                    //.put("client.transport","DEBUG")
                    .build());
            //ESLoggerFactory.getLogger(Netty4Utils.class).catching(org.apache.logging.log4j.Level.DEBUG, (new Throwable()));
            /* client = new PreBuiltXPackTransportClient(Settings.builder()
		        .put("cluster.name", "swarnendu")
		        .put("xpack.security.user", "elastic:changeme")
		        .put("xpack.security.transport.ssl.enabled", "true")
		        .put("xpack.ssl.certificate_authorities", "C:/D/work-space/mcube-2-5/scheduler/test-maven/ca/ca/ca.crt")
		        .build());*/
            //client.addTransportAddress(new TransportAddress(InetAddress.getByName("172.20.25.22"), 9300));
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));

            /*String token = UsernamePasswordToken.basicAuthHeaderValue("elastic", new SecureString("changeme".toCharArray()));

			client.filterWithHeader(Collections.singletonMap("Authorization", token))
			.prepareSearch().get();	*/
            System.out.println("end---");
        } catch (Exception ex) {
            //ex.printStackTrace();
            log.error("",ex);
        }
        return client;

    }

    public static void mapping(Client client) {
        try {
            System.out.println("create mapping");

            client.admin().indices().prepareCreate("twitter3")
                    .setSettings(Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0))
                    .get();

            client.admin().indices().preparePutMapping("twitter").setType("tweet").setSource(
                    "{\n" + "  \"properties\": {\n" + "    \"name\": {\n" + "      \"type\": \"text\"\n" + "    }\n,"
                    + "    \"postDate\": {\n" + "      \"type\": \"date\"\n," + "      \"format\": \"yyyy-MM-dd\"\n"
                    + "    }\n,"
                    + "    \"message\": {\n" + "      \"type\": \"text\"\n" + "    }\n"
                    + "  }\n" + "}",
                    XContentType.JSON).get();

            System.out.println("END create mapping");
        } catch (Exception e) {
            //e.printStackTrace();
            log.error("", e);
        }

    }

    public static void bulkinsert(Client client) {
        try {
            System.out.println("insert data");
            BulkRequestBuilder bulkRequest = client.prepareBulk();

            // either use client#prepare, or use Requests# to directly build
            // index/delete requests
            bulkRequest.add(client.prepareIndex("twitter3", "tweet", "1")
                    .setSource(jsonBuilder().startObject().field("name", "kimchy").field("postDate", "2018-08-31")
                            .field("message", "trying out Elasticsearch").endObject()));

            bulkRequest.add(client.prepareIndex("twitter", "tweet", "2").setSource(jsonBuilder().startObject()
                    .field("name", "kimchy").field("postDate", "2018-08-31").field("message", "another post").endObject()));

            BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                // process failures by iterating through each bulk response item
            }

            System.out.println("END insert data");
        } catch (Exception e) {
           // e.printStackTrace();
            log.error("",e);
        }
    }

    /*public static void test(){
		String token = basicAuthHeaderValue("test_user", new SecureString("x-pack-test-password".toCharArray()));

		client.filterWithHeader(Collections.singletonMap("Authorization", token))
		.prepareSearch().get();
		
	}*/
}
