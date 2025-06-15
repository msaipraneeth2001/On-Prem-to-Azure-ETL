# Great Expectations on Databricks Integration Guide

This guide provides instructions on how to integrate Great Expectations (GX) checkpoints into your Databricks notebooks for data validation in Bronze-to-Silver and Silver-to-Gold ETL/ELT pipelines.

## Prerequisites

1.  **Great Expectations Setup**: Ensure you have a Great Expectations project initialized and configured within your project repository. This guide assumes your `great_expectations` directory (containing `great_expectations.yml`, `expectations/`, `checkpoints/`, etc.) is accessible from your Databricks environment, for example, by cloning the repository to DBFS.
2.  **Databricks Environment**: A Databricks workspace with clusters capable of running PySpark and installing libraries.

## 1. Installation of Great Expectations

In your Databricks notebook, you need to install the `great_expectations` library. This can typically be done at the beginning of your notebook or as a cluster library.

```python
%pip install great_expectations
```

Ensure the version installed is compatible with the version used to create your GX project (e.g., 1.4.6 as per project setup).

## 2. Initializing the DataContext

To interact with your Great Expectations project, you need to initialize a `DataContext`. The `context_root_dir` parameter must point to the absolute path of your `great_expectations` directory on DBFS.

**Example:**

If your repository `my_project_repo` is cloned to `/dbfs/repos/my_user/my_project_repo/`, then the path to the GX project would be `/dbfs/repos/my_user/my_project_repo/great_expectations/`.

```python
import great_expectations as gx

# Adjust this path to where your 'great_expectations' directory is located on DBFS
gx_project_root_dir = "/dbfs/path/to/your/repo/great_expectations"
# For example: gx_project_root_dir = "/dbfs/FileStore/users/your_user/your_repo/great_expectations"
# Or if using Databricks Repos: gx_project_root_dir = "/Workspace/Repos/your_user/your_repo/great_expectations"


try:
    context = gx.get_context(context_root_dir=gx_project_root_dir)
    print("Great Expectations DataContext loaded successfully.")
except Exception as e:
    print(f"Error loading Great Expectations DataContext: {e}")
    # Consider raising the error or handling it as per your pipeline's needs
    raise
```

**Important**: Ensure the path used in `context_root_dir` is correct for your Databricks environment and how you've made the `great_expectations` project available (e.g., via DBFS mounts, Databricks Repos, or direct copy). Using `%fs ls /path/to/check` can be helpful to verify paths in Databricks notebooks.

## 3. Integrating Checkpoints into ETL/ELT Notebooks

Once the DataContext is loaded, you can run your predefined checkpoints.

### 3.1. Bronze-to-Silver Pipeline (Validating Silver Data)

After your Bronze-to-Silver transformations are complete and the silver data (e.g., `Product` table) is written, you can run the `silver_product_checkpoint` to validate it.

```python
# Assuming 'context' is your loaded Great Expectations DataContext

checkpoint_name = "silver_product_checkpoint"
print(f"Running checkpoint: {checkpoint_name}...")

try:
    validation_result = context.run_checkpoint(checkpoint_name=checkpoint_name)
    print(f"Checkpoint run completed. Success: {validation_result.success}")

    if not validation_result.success:
        print("Validation failed!")
        # Programmatic access to results (optional):
        # For detailed inspection, you can explore the validation_result object.
        # For example, to see which expectations failed:
        # for run_result in validation_result.run_results.values():
        #     for validation_result_item in run_result["validation_result"]["results"]:
        #         if not validation_result_item["success"]:
        #             print(f"  Expectation Failed: {validation_result_item['expectation_config']['expectation_type']} on column '{validation_result_item['expectation_config']['kwargs'].get('column')}'")

        # Decide how to handle the failure:
        # Option 1: Raise an exception to stop the pipeline
        # raise Exception(f"Validation checkpoint '{checkpoint_name}' failed. Check Data Docs for details.")

        # Option 2: Log the failure and continue (e.g., if warnings are acceptable)
        # print(f"WARNING: Validation checkpoint '{checkpoint_name}' failed. Pipeline will continue.")

        # Option 3: Send notifications (e.g., via email, Slack - requires additional setup)
        # send_notification(f"Validation checkpoint '{checkpoint_name}' failed.")

    else:
        print("Validation successful!")

except Exception as e:
    print(f"Error running checkpoint '{checkpoint_name}': {e}")
    # Handle checkpoint execution errors (e.g., misconfiguration, data access issues)
    raise

```

### 3.2. Silver-to-Gold Pipeline (Validating Gold Data)

Similarly, after your Silver-to-Gold transformations are complete and the gold data (e.g., `Product` table) is written, run the `gold_product_checkpoint`.

```python
# Assuming 'context' is your loaded Great Expectations DataContext

checkpoint_name = "gold_product_checkpoint"
print(f"Running checkpoint: {checkpoint_name}...")

try:
    validation_result = context.run_checkpoint(checkpoint_name=checkpoint_name)
    print(f"Checkpoint run completed. Success: {validation_result.success}")

    if not validation_result.success:
        print("Validation failed!")
        # Add similar error handling and result inspection as in the silver pipeline example
        # raise Exception(f"Validation checkpoint '{checkpoint_name}' failed. Check Data Docs for details.")
    else:
        print("Validation successful!")

except Exception as e:
    print(f"Error running checkpoint '{checkpoint_name}': {e}")
    # Handle checkpoint execution errors
    raise
```

## 4. General Considerations

### 4.1. Path to `great_expectations` Directory

As mentioned, the `context_root_dir` in `gx.get_context(context_root_dir=...)` is critical.
- If using **Databricks Repos**, the path will typically start with `/Workspace/Repos/`.
- If you've manually copied or synced your project to **DBFS**, it might be `/dbfs/FileStore/` or another DBFS path.
- Ensure your Spark cluster has the necessary permissions to access this path and the underlying data paths specified in your datasource configurations (e.g., `/mnt/silver/`, `/mnt/gold/`).

### 4.2. Error Handling and Pipeline Impact

How you react to `validation_result.success == False` is application-specific:
-   **Critical Pipelines**: You might want to raise an exception and stop the ETL/ELT process immediately to prevent downstream corruption.
-   **Monitoring/Reporting**: For some pipelines, logging the failure and sending notifications might be sufficient, allowing the pipeline to continue.
-   **Quarantining Data**: Advanced workflows might involve moving failed batches of data to a quarantine area for investigation.

### 4.3. Accessing Data Docs

Great Expectations generates Data Docs, which are HTML reports detailing expectation suites, validation results, and profiling information.
-   **Local Storage (Default for this Setup)**: The current project is configured to store Data Docs locally within the `great_expectations/uncommitted/data_docs/local_site/` directory.
    -   If your `great_expectations` project is on DBFS, you can access these files. You might need to sync them from DBFS to your local machine or use Databricks utilities to serve/view HTML from DBFS if possible (though this can be complex).
    -   A common pattern is to have a CI/CD process that runs GX checkpoints and publishes the Data Docs to a static website hosting service (e.g., AWS S3, Azure Blob Storage, GitHub Pages).
-   **Shared Storage**: For team access, configure your Data Docs store to a shared location like S3, Azure Blob Storage, etc. This involves changing the `store_backend` for `local_site` (or adding a new site) in `great_expectations.yml`.

Refer to the Great Expectations documentation for detailed instructions on configuring Data Docs hosting.

## 5. Datasource Configuration for Databricks

This project's `great_expectations.yml` uses `SparkDFExecutionEngine`. The datasources (`spark_silver_datasource`, `spark_gold_datasource`) were configured with:
-   `InferredAssetFilesystemDataConnector` for silver (as a workaround during setup).
-   `InferredAssetDBFSDataConnector` for gold (as per original intent).

When running in Databricks, `InferredAssetDBFSDataConnector` is generally the correct choice for data on DBFS or DBFS-accessible mounts. Ensure your Spark session in Databricks can access the specified `base_directory` paths (e.g., `/mnt/silver/SalesLT/`, `/mnt/gold/SalesLT/`). If you encounter issues with `InferredAssetDBFSDataConnector` (like schema parsing errors during `gx.get_context`), ensure its configuration in `great_expectations.yml` is correct and that it's compatible with your GX version. During the setup of this project, there were some difficulties getting `DataContext` to load with `InferredAssetDBFSDataConnector`, which might indicate a need for careful review of its configuration or potential version sensitivities.

## Conclusion

Integrating Great Expectations checkpoints into your Databricks notebooks provides robust data quality gates. Remember to adapt paths and error handling strategies to your specific environment and pipeline requirements.
