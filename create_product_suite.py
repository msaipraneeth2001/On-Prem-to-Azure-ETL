import great_expectations as gx
from great_expectations.core.batch import BatchRequest
# No need to import ExpectationSuite for this approach if get_validator handles creation

def create_silver_product_suite():
    # 1. Load DataContext
    context = gx.get_context(context_root_dir="great_expectations")
    print("DataContext loaded.")

    # 2. Define Suite Name
    suite_name = "silver_product_suite"
    print(f"Target Expectation Suite: '{suite_name}'.")

    # 3. Construct BatchRequest
    # This BatchRequest refers to a datasource that needs to be active in great_expectations.yml
    batch_request = BatchRequest(
        datasource_name="spark_silver_datasource",
        data_connector_name="delta_silver_connector",
        data_asset_name="Product",
    )
    print("BatchRequest constructed.")

    # 4. Get Validator
    # For this to work AND create the suite, the 'spark_silver_datasource' must be correctly
    # configured and loadable in great_expectations.yml.
    # get_validator should create the suite if it doesn't exist by this name.
    print(f"Attempting to get validator for suite '{suite_name}'...")
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name  # Pass the suite name as a string
    )
    print(f"Validator obtained. Active suite: '{validator.expectation_suite_name}'.")

    # 5. Add Expectations
    print("Adding expectations...")
    expected_columns = [
        "ProductID", "Name", "ProductNumber", "Color", "StandardCost", "ListPrice",
        "Size", "Weight", "ProductCategoryID", "ProductModelID", "SellStartDate",
        "SellEndDate", "DiscontinuedDate", "rowguid", "ModifiedDate"
    ]
    for col in expected_columns:
        validator.expect_column_to_exist(column=col)
    print(f"Added {len(expected_columns)} 'expect_column_to_exist' expectations.")

    not_expected_columns = ["ThumbNailPhoto", "thumbnailphotofilename"]
    for col in not_expected_columns:
        validator.expect_column_to_not_exist(column=col)
    print(f"Added {len(not_expected_columns)} 'expect_column_to_not_exist' expectations.")

    date_columns = ["SellStartDate", "SellEndDate", "DiscontinuedDate", "ModifiedDate"]
    strict_date_regex = r"^\d{4}-\d{2}-\d{2}$"
    for col in date_columns:
        validator.expect_column_values_to_be_of_type(column=col, type_="StringType")
        validator.expect_column_values_to_match_regex(column=col, regex=strict_date_regex)
    print(f"Added type and regex expectations for {len(date_columns)} date columns.")

    # 6. Save Suite using the validator
    print("Saving Expectation Suite using validator...")
    validator.save_expectation_suite(discard_failed_expectations=False)
    print(f"Expectation Suite '{suite_name}' saved successfully via validator.")

if __name__ == "__main__":
    create_silver_product_suite()
