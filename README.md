# API Generator Samples

This repository contains artifacts for automating API development, as described in the blog post "Automating API Development with AI to Accelerate Time-to-Market." These resources streamline API creation by parsing an API mapping document to generate a comprehensive API endpoint.

## Files
- **test_mapping.csv**: A sample API mapping document defining fields (e.g., `customer_id`, `email`, `subscriptions[].opt_status`) with validations (e.g., required, conditional, enum:Y,N).
- **ParserFunction.py**: A Python script that parses the CSV and generates a Lambda function with a Flask-like API endpoint, supporting all validations (e.g., data types, max length, conditional rules), deployable in AWS Lambda.

## Usage
1. **API Mapping Document**:
   - Modify `test_mapping.csv` to include your fields and validations.
   - Required columns: `api_field`, `table`, `dynamodb_column`, `data_type`, `max_length`, `required`, `transformation`.
2. **ParserFunction Script**:
   - Deploy in a serverless environment (e.g., AWS Lambda) triggered by an S3 upload.
   - Generates a Lambda function with validations for all CSV fields.
   - Adapt the generated code for other frameworks (e.g., FastAPI) or databases (e.g., MySQL).
3. **Testing**:
   - Test locally with `test_mapping.csv` to generate an API endpoint.
   - Deploy the generated code to a Lambda function with API Gateway.
   - Test with sample data to verify validations and DynamoDB storage.

## Extensibility
- Add validations (e.g., regex, date formats) to the CSV.
- Modify `ParserFunction.py` for Java or .NET code generation.
- Enhance the generated code with custom error handling.

These artifacts can help reduce client onboarding time by up to 80% and accelerate time-to-market, as demonstrated in our project.
