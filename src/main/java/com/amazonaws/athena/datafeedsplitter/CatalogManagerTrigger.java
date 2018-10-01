package com.amazonaws.athena.datafeedsplitter;


import com.amazonaws.services.lambda.invoke.LambdaFunction;
import com.amazonaws.services.lambda.invoke.LambdaFunctionNameResolver;
import com.amazonaws.services.lambda.invoke.LambdaInvokerFactoryConfig;
import com.amazonaws.services.lambda.model.InvocationType;

import java.lang.reflect.Method;

/**
 * CatalogManagerInput
 *
 * Define the set of parameters required for calling the Catalog manager lambda function.
 * These values get marshaled into JSON when called by LambdaInvokerFactory.
 */
class CatalogManagerInput {

    /** The S3 URI of the base of a Data Feed report. */
    private String s3ReportBase;

    /** The S3 URI of where the latest lookup files are. */
    private String s3LookupUri;

    /** The date of the report in YYYY-mm-dd format. */
    private String reportDate;

    /** getter for s3ReportBase. */
    public String getReportBase() { return s3ReportBase; }

    /** setter for S3ReportBase. */
    public void setReportBase(final String value) { s3ReportBase = value; }

    /** getter for s3LookupURI. */
    public String getLookupURI() { return s3LookupUri; }

    /** setter for s3LookupURI. */
    public void setLookupURI(final String value) { s3LookupUri = value; }

    /** getter for reportDate. **/
    public String getReportDate() { return reportDate; }

    /** setter for reportDate. **/
    public void setReportDate(final String value) { reportDate = value; }

}

/**
 * CatalogManagerTrigger defines an interface consumers can call to invoke the Lambda function.
 */
interface CatalogManagerTrigger {
    @LambdaFunction(invocationType = InvocationType.Event)
    void addParts(CatalogManagerInput input);
}

/**
 * EnvironmentLambdaFunctionNameResolver implements a way to get the Lambda functions name from an environment variable.
 */
class EnvironmentLambdaFunctionNameResolver implements LambdaFunctionNameResolver {
    public final String getFunctionName(
            final Method method, final LambdaFunction annotation, final LambdaInvokerFactoryConfig config)  {
        return System.getenv("CATALOG_MANAGER_LAMBDA");
    }
}