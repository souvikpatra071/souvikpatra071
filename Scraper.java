package com.umusic.datalake.elt.shopify.ingestion.dataflow;

import static com.umusic.datalake.elt.shared.transform.util.CommonUtils.truncateOutputPath;
import static com.umusic.datalake.elt.shopify.ingestion.common.Consts.*;
import static org.apache.beam.sdk.io.FileIO.Write.defaultNaming;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;

import com.google.cloud.bigquery.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import com.bettercloud.vault.Vault;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.umusic.data.datalake.vault.VaultUtil;
import com.umusic.datalake.elt.shared.model.ApiErrorLog;
import com.umusic.datalake.elt.shopify.ingestion.common.Utils;
import com.umusic.datalake.elt.shopify.ingestion.fn.ScrapeHourlyFn;
import com.umusic.datalake.elt.shopify.ingestion.fn.ScrapeUpdateFn;
import com.umusic.datalake.elt.shopify.ingestion.model.ApiParams;
import com.umusic.datalake.elt.shopify.ingestion.options.ScraperOptions;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Scraper {
    //private static final Logger log = LoggerFactory.getLogger(Scraper.class);
    static final Logger LOGGER = Logger.getLogger(Scraper.class.getName());
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final ObjectMapper jsonMapper = new ObjectMapper().configure(JsonGenerator.Feature.WRITE_NUMBERS_AS_STRINGS, true);
    public static String outputPath;
    private static String reportDate;
    private static String execDay;
    private static String frequency;
    private static Map<String, Map<String, String>> shops;
    private static Set<String> apisToSkipSet;
    
    public static void runDownloadUrls(ScraperOptions options) throws JsonProcessingException {
        try {
        	final String vaultToken=
        			VaultUtil.getTokenByServiceAccount
        			(options.getVaultUrl(), options.getVaultRole(),
                            String.format("projects/%s/serviceAccounts/%s", options.getProject(), options.getVaultServiceAccount()));
            Vault vault = VaultUtil.buildVault(options.getVaultUrl(), vaultToken);
            shops = VaultUtil.getNestKVMap(vault, options.getVaultShopifyPath());
        } catch (Exception e) {
        	LOGGER.log(Level.SEVERE, e.getMessage());
        }

        LOGGER.log(Level.INFO, "Count of Shops fetched from vault" + shops.size());

        apisToSkipSet = new HashSet<>(Arrays.asList(options.getApisToSkip().split(",")));

        FileSystems.setDefaultPipelineOptions(options);

        Boolean isBqSync = Boolean.valueOf(options.getBQSyncFlag());

        if(isBqSync) {
            String query = BQ_SYNC_FLAG_QUERY;

            try {
                BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

                QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

                TableResult results = bigquery.query(queryConfig);

                for (FieldValueList row : results.iterateAll()) {

                    String key_name = row.get(0).getStringValue();

                    if (shops.containsKey(key_name)) {
                        shops.remove(key_name);
                    }

                }

            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error in removing Inactive shops from bq sync" + e.getMessage());
            }
        }

        LOGGER.log(Level.INFO, "total number of stores after removing inactive stores" + shops.size());

        frequency = options.getFrequency();
        execDay = LocalDate.parse(options.getStartDate().split("T")[0]).getDayOfWeek().toString();
        
        outputPath = options.getOutputPath();
        if (frequency == null || frequency.isEmpty() || frequency.equals("daily") )
            reportDate = LocalDate.parse(options.getEndDate()).minusDays(1).toString();
        else // hourly
            reportDate = options.getStartDate();

        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> collection;
        /**
         * check if error state file exists
         */
        String errorStatePath = options.getErrorStatePath();
        if (getFileCounts(errorStatePath) >= 1) {
            collection = pipeline.apply("read error state", TextIO.read().from(getParentDir(errorStatePath) + "*error*"))
                    .apply(ParDo.of(new DoFn<String, String>() {
                        @ProcessElement
                        public void processElement(ProcessContext ctx) throws Exception {
                            ctx.output(mapper.writeValueAsString(((ApiErrorLog<ApiParams>) (mapper.readValue(ctx.element(), new TypeReference<ApiErrorLog<ApiParams>>() {
                            }))).getContext()));
                        }
                    }));
        } else {
            truncateOutputPath(outputPath);
            if(!options.getInputPath().startsWith("gs://")) {
                List<String> urlsToScrape = new ArrayList();
                for (Map.Entry<String, Map<String, String>> shop : shops.entrySet()) {
                	String apiUrl;
                    String shopDomain = shop.getKey();
                    String accessToken = shop.getValue().get("access_token");
                    if(accessToken == null) {
                        continue;
                    }
                    if (shopDomain.equalsIgnoreCase(options.getBackfillShop())){
                        for (KV<String, String> api : getApiList(frequency,execDay)) {
                            String apiKey = api.getKey();
                            /* ADDED CODE FOR ADD THE START DATE +2 FOR PAGES URL ALONE JIRA */
                            if(( PAGES.equalsIgnoreCase(apiKey) ||  PAYOUTS.equalsIgnoreCase(apiKey)) && START_DAY.equalsIgnoreCase(
                                    LocalDate.parse(options.getStartDate().split("T")[0]).getDayOfWeek().toString())){
                                String salesStartDate;
                                apiUrl = api.getValue().replace("{shop_domain}", shopDomain)
                                .replace("{start_date}",LocalDate.parse(options.getStartDate()).minusDays(2).toString() )
                                // .replace("{access_token}", accessToken)
                                .replace("{shopify_api_version}", options.getApiVersion());
                                }
                            /* ADDED CODE FOR ENDS HERE */
                            else
                            {
                                apiUrl = api.getValue().replace("{shop_domain}", shopDomain)
                                        .replace("{start_date}", options.getStartDate())
                                    //  .replace("{access_token}", accessToken)
                                        .replace("{shopify_api_version}", options.getApiVersion());
                                
                            }
                            if(api.getKey().equals("payouts"))
                                apiUrl = apiUrl.replace("{end_date}", reportDate).replace("{shopify_api_version}", options.getApiVersion());
                            else
                                apiUrl = apiUrl.replace("{end_date}", options.getEndDate()).replace("{shopify_api_version}", options.getApiVersion());;
                            
                            urlsToScrape.add(mapper.writeValueAsString(new ApiParams(apiUrl, shopDomain, api.getKey(), "")));
                            
                        }
                    }
                }
                collection = pipeline.apply(Create.of(urlsToScrape));
            } else {
                collection = pipeline.apply("read second round url", TextIO.read().from(options.getInputPath()))
                        .apply("dedup second round url", Distinct.create())
                        .apply(ParDo.of(new DoFn<String, String>() {
                            @ProcessElement
                            public void processElement(ProcessContext ctx) throws Exception {
                                ctx.output(ctx.element());
                            }
                        }));
            }
        }
        PCollection<KV<String, String>> scrapeResult = collection.apply("SplitByKey",
				ParDo.of(new DoFn<String, KV<String, String>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						try {

							ApiParams params = mapper.readValue(c.element(), new TypeReference<ApiParams>() {
							});
							String shopDomain = params.getShopDomain();
							if (shopDomain != null && !shopDomain.isEmpty()) {
							c.output(KV.of(shopDomain, c.element()));
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}))
        		.apply(Distinct.<KV<String, String>>create())
        		.apply(GroupByKey.<String, String>create()).apply("scrape", ParDo.of(getScapeFn(frequency, options)));
        
        PCollection<KV<String, String>> errorMsg = null;
       // PCollection<KV<String, String>> errorMsg1 = null;
        // boolean hourlyFlag = getApiList1(frequency);
        // boolean bool = false;
       // if(!hourlyFlag) {
        	errorMsg = scrapeResult.apply("filter error urls",
                Filter.by((SerializableFunction<KV<String, String>, Boolean>) kv -> kv.getKey().equals("error")));
//        }
//        else if(hourlyFlag && !options.getErrorStatePath().toLowerCase().contains("datalake-shopify-api-download-shopify-1")){
//        	errorMsg = scrapeResult.apply("filter error urls",
//                    Filter.by((SerializableFunction<KV<String, String>, Boolean>) kv -> kv.getKey().equals("error") && !kv.getValue().contains("429")));
//        	errorMsg1 = scrapeResult.apply("filter error urls 429",
//                    Filter.by((SerializableFunction<KV<String, String>, Boolean>) kv -> kv.getKey().equals("error")&& kv.getValue().contains("429")));
//        }else {
//        	errorMsg = scrapeResult.apply("filter error urls",
//                    Filter.by((SerializableFunction<KV<String, String>, Boolean>) kv -> kv.getKey().equals("error")));
//		errorMsg1 = scrapeResult.apply("filter error urls 429",
//                    Filter.by((SerializableFunction<KV<String, String>, Boolean>) kv -> kv.getKey().equals("error")&& kv.getValue().contains("429")));
//        }
        PCollection<KV<String, String>> report = scrapeResult.apply("filter report urls",
                Filter.by((SerializableFunction<KV<String, String>, Boolean>) kv -> kv.getKey().equals("report")));
        PCollection<KV<String, String>> data = scrapeResult.apply("filter http 200",
                Filter.by((SerializableFunction<KV<String, String>, Boolean>) kv -> !(kv.getKey().equals("error")) && !(kv.getKey().equals("report"))));
        
        PCollection<String> outputPathPCollection = pipeline.apply("Create Output Path Pcollection", Create.of(outputPath).withCoder(StringUtf8Coder.of()));
        PCollectionView<String> view = outputPathPCollection.apply(View.<String>asSingleton());
        
//         if(hourlyFlag && !options.getErrorStatePath().toLowerCase().contains("datalake-shopify-api-download-shopify-1")){
//         	data.apply("write http response", ParDo.of(new DoFn <KV<String, String>, String>() 
// 			      {		private static final long serialVersionUID = 1L;
			      
					
// 					@ProcessElement
// 				       public void processElement(ProcessContext c, OutputReceiver<String> o) throws JsonProcessingException, IOException
// 					{	
// 						ByteArrayOutputStream baos = new ByteArrayOutputStream();
// 						Random random = new Random();
// 						String outputKey = c.sideInput(view);
// 						String key = c.element().getKey();
// 						String value = c.element().getValue();
// 						String orderId = null;
						
// 						JsonNode node = jsonMapper.readTree(value);
// 						if(node != null) {
// 							orderId = node.get("order_id").asText();
// 						}
						
// 						baos.write(value.getBytes("UTF-8"));
						
// 						Utils.writeTextToGcs(
// 								outputKey+key+"/"+orderId+"-"+System.currentTimeMillis()+random.nextInt()+".json",
// 								new ByteArrayInputStream(baos.toByteArray()));
// 					}
// 					}).withSideInputs(view));
//         }else {
        	data.apply("write http response", FileIO.<String, KV<String, String>>writeDynamic()
                .by((SerializableFunction<KV<String, String>, String>) input ->
                        input.getKey() + "/" + input.getKey())
                .via(Contextful.fn((SerializableFunction<KV<String, String>, String>) input ->
                        input.getValue()), TextIO.sink())
                .to(outputPath)
                .withDestinationCoder(StringUtf8Coder.of())
               // .withNumShards(1)
                .withNaming(path -> defaultNaming(path, "-" + System.currentTimeMillis() + ".json")));
       // }
        
//        if(hourlyFlag && !options.getErrorStatePath().toLowerCase().contains("datalake-shopify-api-download-shopify-1")) {  
//        	PCollection<KV<String, String>> errorMsg2 = retryError(errorMsg1, options, 1, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg3 = retryError(errorMsg2, options, 2, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg4 = retryError(errorMsg3, options, 3, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg5 = retryError(errorMsg4, options, 4, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg6 = retryError(errorMsg5, options, 5, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg7 = retryError(errorMsg6, options, 6, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg8 = retryError(errorMsg7, options, 7, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg9 = retryError(errorMsg8, options, 8, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg10 = retryError(errorMsg9, options, 9, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg11 = retryError(errorMsg10, options, 10, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg12 = retryError(errorMsg11, options, 11, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg13 = retryError(errorMsg12, options, 12, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg14 = retryError(errorMsg13, options, 13, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg15 = retryError(errorMsg14, options, 14, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg16 = retryError(errorMsg15, options, 15, view, hourlyFlag);
//        }else if(hourlyFlag && options.getErrorStatePath().toLowerCase().contains("datalake-shopify-api-download-shopify-1")) { 
//		PCollection<KV<String, String>> errorMsg2 = retryError(errorMsg1, options, 1, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg3 = retryError(errorMsg2, options, 2, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg4 = retryError(errorMsg3, options, 3, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg5 = retryError(errorMsg4, options, 4, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg6 = retryError(errorMsg5, options, 5, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg7 = retryError(errorMsg6, options, 6, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg8 = retryError(errorMsg7, options, 7, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg9 = retryError(errorMsg8, options, 8, view, hourlyFlag);
//        	PCollection<KV<String, String>> errorMsg10 = retryError(errorMsg9, options, 9, view, hourlyFlag);
//	}
	    
	errorMsg.apply("get value - errors", ParDo.of(new DoFn<KV<String, String>, String>() {
            @ProcessElement
            public void processElement(ProcessContext ctx) {
                ctx.output(ctx.element().getValue());
            }
        })).apply("write failed requests", TextIO.write().to(options.getErrorStatePath()).withoutSharding());

        report.apply("get value - reports", ParDo.of(new DoFn<KV<String, String>, String>() {
            @ProcessElement
            public void processElement(ProcessContext ctx) {
                ctx.output(ctx.element().getValue());
            }
        })).apply("dedup unavialble stores", Distinct.create()).apply("report unavailable stores",
                TextIO.write().to(options.getReportPath() + System.currentTimeMillis() + ".txt").withoutSharding());      
       
        pipeline.run();
    }
    
//    private static PCollection<KV<String, String>> retryError(PCollection<KV<String, String>> errorMsg1, ScraperOptions options, int i, PCollectionView<String> view, boolean hourlyFlag) {
//    	PCollection<KV<String, String>> scrapeResult = errorMsg1.apply("split as apiparams "+i, ParDo.of(new DoFn<KV<String, String>, String>() {
//            @ProcessElement
//            public void processElement(ProcessContext ctx) throws Exception {
//                ctx.output(mapper.writeValueAsString(((ApiErrorLog<ApiParams>) (mapper.readValue(ctx.element().getValue(), new TypeReference<ApiErrorLog<ApiParams>>() {
//                }))).getContext()));
//            }
//        })).apply("SplitByKey",
//				ParDo.of(new DoFn<String, KV<String, String>>() {
//					private static final long serialVersionUID = 1L;
//
//					@ProcessElement
//					public void processElement(ProcessContext c) {
//						try {
//
//							ApiParams params = mapper.readValue(c.element(), new TypeReference<ApiParams>() {
//							});
//							String shopDomain = params.getShopDomain();
//							if (shopDomain != null && !shopDomain.isEmpty()) {
//								c.output(KV.of(shopDomain, c.element()));
//								}
//						} catch (Exception e) {
//							e.printStackTrace();
//						}
//					}
//				})).apply(Distinct.<KV<String, String>>create()).apply(GroupByKey.<String, String>create()).apply("scrape Fn call"+i, ParDo.of(getScapeFn(frequency, options)));
//    	
//    	PCollection<KV<String, String>>  errorMsg = scrapeResult.apply("filter not 429 error urls "+i,
//                Filter.by((SerializableFunction<KV<String, String>, Boolean>) kv -> kv.getKey().equals("error") && !kv.getValue().contains("429")));
//    	PCollection<KV<String, String>>  errorMsg2 = scrapeResult.apply("filter 429 urls "+i,
//                Filter.by((SerializableFunction<KV<String, String>, Boolean>) kv -> kv.getKey().equals("error")&& kv.getValue().contains("429")));
//    	
//    	PCollection<KV<String, String>> report = scrapeResult.apply("filter retry report urls "+i,
//                Filter.by((SerializableFunction<KV<String, String>, Boolean>) kv -> kv.getKey().equals("report")));
//        PCollection<KV<String, String>> data = scrapeResult.apply("filter retry success "+i,
//                Filter.by((SerializableFunction<KV<String, String>, Boolean>) kv -> !(kv.getKey().equals("error")) && !(kv.getKey().equals("report"))));
//
//        if(hourlyFlag && !options.getErrorStatePath().toLowerCase().contains("datalake-shopify-api-download-shopify-1")){
//        	data.apply("write http response", ParDo.of(new DoFn <KV<String, String>, String>() 
//	      {		private static final long serialVersionUID = 1L;
//			
//			@ProcessElement
//		       public void processElement(ProcessContext c, OutputReceiver<String> o) throws JsonProcessingException, IOException
//			{	
//				ByteArrayOutputStream baos = new ByteArrayOutputStream();
//				Random random = new Random();
//				String value = c.element().getValue();
//				String key = c.element().getKey();
//				String orderId = null;
//				
//				JsonNode node = jsonMapper.readTree(value);
//				if(node != null) {
//					orderId = node.get("order_id").asText();
//				}
//				
//				String outputKey = c.sideInput(view);
//				baos.write(value.getBytes("UTF-8"));
//				
//				Utils.writeTextToGcs(
//						outputKey+key+"/"+orderId+"-"+System.currentTimeMillis()+random.nextInt()+".json",
//						new ByteArrayInputStream(baos.toByteArray()));
//			}
//			}).withSideInputs(view));
//        }else {
//             data.apply("write as file "+i, FileIO.<String, KV<String, String>>writeDynamic()
//                .by((SerializableFunction<KV<String, String>, String>) input ->
//                        input.getKey() + "/" + input.getKey())
//                .via(Contextful.fn((SerializableFunction<KV<String, String>, String>) input ->
//                        input.getValue()), TextIO.sink())
//                .to(outputPath)
//                .withDestinationCoder(StringUtf8Coder.of())
//                .withNumShards(1)
//                .withNaming(path -> defaultNaming(path, "-" + System.currentTimeMillis() + ".json")));
//        }
//
//
//        errorMsg.apply("write failed file "+i, ParDo.of(new DoFn<KV<String, String>, String>() {
//            @ProcessElement
//            public void processElement(ProcessContext ctx) {
//                ctx.output(ctx.element().getValue());
//            }
//        })).apply("write failed requests "+i, TextIO.write().to(options.getErrorStatePath()).withoutSharding());
//
//        report.apply("wite report file "+i, ParDo.of(new DoFn<KV<String, String>, String>() {
//            @ProcessElement
//            public void processElement(ProcessContext ctx) {
//                ctx.output(ctx.element().getValue());
//            }
//        })).apply("dedup retry report "+i, Distinct.create()).apply("write unavialble reports "+i,
//                TextIO.write().to(options.getReportPath() + System.currentTimeMillis() + ".txt").withoutSharding());
//        
//        return errorMsg2;
//    }

   
    	 private static long getFileCounts(String rawPath) {
    	        String dir = getParentDir(rawPath);
    	        if(dir.startsWith("gs:")) {
    	            Storage storage = StorageOptions.getDefaultInstance().getService();
    	            String path = dir.split("gs://")[1];
    	            String[] pathElement = path.split("/", 2);
    	            String bucketName = pathElement[0];
    	            String blobName = pathElement[1];
    	            Page<Blob> blobs = storage.list(bucketName, Storage.BlobListOption.currentDirectory(),
    	                    Storage.BlobListOption.prefix(blobName));
    	            int count = 0;
    	            for(Blob blob: blobs.iterateAll()) {
    	            	if(blob.getName().toString().contains(reportDate.split("\\+")[0]))
    	                count ++;
    	            }
    	            return count;
    	        } else {
    	            File file = new File(dir);
    	            return Arrays.asList(file.list()).stream().filter(x -> !x.startsWith(".")).count();
    	        }
    	    }
    	 private static String getParentDir(String filename) {
 	        return filename.substring(0, filename.lastIndexOf("/") + 1);
 	    }
	

    	   

	private static List<KV<String, String>> getApiList(String frequency , String execDay) {
        if (frequency == null || frequency.isEmpty()) {
        return API_ARR;
        } 
        else if (frequency.equals("daily")) {
        //"hourly"
            if (execDay.equals("SATURDAY") || execDay.equals("SUNDAY")) {
            return API_ARR_SAT_AND_SUN;} 
            else {
            return API_ARR_DAILY_MON_TO_FRI;}}
        else {
        //"hourly"
        return API_ARR_HOURLY;
        }
    }
	
// 	private static boolean getApiList1(String frequency) {
//         if (frequency == null || frequency.isEmpty())
//             return false;
//         else if (frequency.equals("daily"))
//             return false;
//         else // "hourly"
//             return true;
//     }

    private static DoFn<KV<String, Iterable<String>>, KV<String, String>> getScapeFn(String frequency, ScraperOptions options) {
        if (frequency == null || frequency.isEmpty() || frequency.equals("daily"))
            return new ScrapeUpdateFn(reportDate, shops, apisToSkipSet, options.getApiVersion());
        else // "hourly"
            return new ScrapeHourlyFn(reportDate, shops, apisToSkipSet, options.getApiVersion(), options.getOrderMetafieldsApisToFetch());
    }

    public static void main(String[] args) throws JsonProcessingException {
        PipelineOptionsFactory.register(ScraperOptions.class);
        ScraperOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(ScraperOptions.class);

        runDownloadUrls(options);
    }
}
