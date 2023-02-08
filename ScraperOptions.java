package com.umusic.datalake.elt.shopify.ingestion.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface ScraperOptions extends DataflowPipelineOptions {

    @Description("updated_at_min, in yyyy-MM-dd format")
    @Validation.Required
    String getStartDate();
    void setStartDate(String value);

    @Description("updated_at_max, in yyyy-MM-dd format")
    @Validation.Required
    String getEndDate();
    void setEndDate(String value);

    @Description("output base path")
    @Validation.Required
    String getOutputPath();
    void setOutputPath(String value);

    @Description("error state path that logs failed api calls")
    @Validation.Required
    String getErrorStatePath();
    void setErrorStatePath(String value);

    @Description("report unavailable stores")
    @Validation.Required
    String getReportPath();
    void setReportPath(String value);

    @Description("input base path")
    String getInputPath();
    void setInputPath(String value);

    @Description("vault url")
    @Validation.Required
    String getVaultUrl();
    void setVaultUrl(String value);

    @Description("vault token")
    String getVaultToken();
    void setVaultToken(String value);

    @Description("vault shopify path")
    @Validation.Required
    String getVaultShopifyPath();
    void setVaultShopifyPath(String value);

    @Description("apis to skip")
    @Validation.Required
    String getApisToSkip();
    void setApisToSkip(String value);

    @Description("order metafields apis to fetch")
    String getOrderMetafieldsApisToFetch();
    void setOrderMetafieldsApisToFetch(String value);

    @Description("pipeline frequency")
    String getFrequency();
    void setFrequency(String frequency);
    
    @Description("api version")
    @Validation.Required
    String getApiVersion();
    void setApiVersion(String apiVersion);
    
    
    @Description("vault role")
    @Validation.Required
    String getVaultRole();
    void setVaultRole(String value);
    
    @Description("vault ServiceAccount")
    @Validation.Required
    String getVaultServiceAccount();
    void setVaultServiceAccount(String value);

    @Description("Flag to determine if shops need to be filtered from BQ")
    @Validation.Required
    String getBQSyncFlag();
    void setBQSyncFlag(String value);

    @Description("Shops going to backfill")
    @Validation.Required
    String getBackfillShop();
    void setBackfillShop(String value);
}
