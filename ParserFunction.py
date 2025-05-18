import json
import boto3
import csv
import io
import logging
import re

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
bedrock_client = boto3.client('bedrock-runtime', region_name='us-east-1')
lambda_client = boto3.client('lambda')

def clean_code(code):
    code = re.sub(r'```python\n|```', '', code, flags=re.MULTILINE)
    code = re.sub(r'^\s*#+.*$', '', code, flags=re.MULTILINE)
    code = code.strip()
    lines = code.split('\n')
    cleaned_lines = []
    i = 0
    while i < len(lines):
        line = lines[i].rstrip()
        if line.strip().startswith('try:'):
            found_handler = False
            for j in range(i + 1, min(i + 10, len(lines))):
                if lines[j].strip().startswith(('except', 'finally')):
                    found_handler = True
                    break
            if not found_handler:
                logger.warning(f"Removing incomplete try block at line {i + 1}")
                i = j if j < len(lines) else len(lines)
                continue
        cleaned_lines.append(line)
        i += 1
    code = '\n'.join(cleaned_lines)
    return code

def validate_code_syntax(code):
    try:
        compile(code, '<string>', 'exec')
        return True
    except SyntaxError as e:
        logger.error(f"Syntax error in generated code: {str(e)} at line {e.lineno}")
        return False

def detect_delimiter(csv_content):
    try:
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(csv_content.split('\n')[0])
        return dialect.delimiter
    except csv.Error:
        logger.warning("Delimiter detection failed, defaulting to comma")
        return ','

def lambda_handler(event, context):
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        logger.info(f"Processing S3 file: s3://{bucket}/{key}")

        response = s3_client.get_object(Bucket=bucket, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        logger.info(f"CSV content (first 100 chars):\n{csv_content[:100]}...")

        delimiter = detect_delimiter(csv_content)
        csv_reader = csv.DictReader(io.StringIO(csv_content), delimiter=delimiter)
        if not csv_reader.fieldnames:
            logger.warning(f"No headers found with delimiter '{delimiter}', trying comma...")
            csv_reader = csv.DictReader(io.StringIO(csv_content), delimiter=',')

        required_headers = {'api_field', 'table', 'dynamodb_column', 'data_type', 'max_length', 'required', 'transformation'}
        if not required_headers.issubset(set(csv_reader.fieldnames or [])):
            missing = required_headers - set(csv_reader.fieldnames or [])
            raise ValueError(f"Missing required headers: {missing}")

        schema = []
        tables = set()
        for row in csv_reader:
            api_field = row['api_field'].strip()
            table = row['table'].strip()
            if not api_field or not table:
                logger.warning(f"Skipping invalid row: {row}")
                continue
            schema.append({
                'api_field': api_field,
                'table': table,
                'dynamodb_column': row['dynamodb_column'].strip(),
                'data_type': row['data_type'].strip(),
                'max_length': row['max_length'].strip() or 'None',
                'required': row['required'].strip(),
                'transformation': row['transformation'].strip()
            })
            tables.add(table)
        schema_json = json.dumps(schema, indent=2)
        tables_json = json.dumps(list(tables), indent=2)
        logger.info(f"Generated schema: {schema_json}")
        logger.info(f"Supported tables: {tables_json}")

        prompt = f"""
You are an expert Python developer. Generate a valid AWS Lambda function (`lambda_function.py`) based on the provided schema for processing API Gateway POST requests to perform CRUD operations on DynamoDB tables. The function must:
1. Handle API Gateway events where the payload is in `event['body']` (JSON string or dict). Use `event.get('body', event)`.
2. Parse the payload with `json.loads` if it’s a string, with try-except for `json.JSONDecodeError`.
3. Expect a payload with an `operations` list, each item containing `table`, `row_key`, `operation` ('create', 'read', 'update', 'delete'), and `data`. Validate these fields.
4. Support CRUD operations for each operation in the list:
   - 'create': Use `put_item` to create a new item.
   - 'read': Use `get_item` to retrieve an item by `row_key`, return 404 if not found.
   - 'update': Use `put_item` to update an item.
   - 'delete': Use `delete_item` to remove an item by `row_key`, check existence.
5. Validate and transform 'data' based on the schema:
   - Map 'api_field' to 'dynamodb_column'.
   - Parse 'dynamodb_column' paths (e.g., 'profile:customer_id' maps to item['profile']['customer_id'], 'subscription:customer_id' to item['subscription']['customer_id']).
   - For array fields (e.g., 'subscriptions[].field'), iterate over the 'subscriptions' list in 'data' and validate each item.
   - Enforce 'data_type' (string, datetime).
   - Check 'max_length' for strings (if not 'None', convert to int).
   - Apply 'required' rules (true, false, conditional).
   - Apply 'transformation' rules:
     - 'trim': Strip whitespace.
     - 'enum:values': Validate against comma-separated values (e.g., 'enum:Y,N').
     - 'require_one:field1,field2': Ensure at least one field is non-empty after trim.
     - 'require_if:field': Require if another field is non-empty.
6. Support nesting for 'profile:field' and 'subscription:field' (e.g., 'subscription:subscriptions[].field' maps to item['subscription']['subscriptions']).
7. Use 4-space indentation, correct Python syntax, and complete try-except blocks for all operations.
8. Return a JSON response with `statusCode` and `body` as a Python dictionary (not a JSON string) to avoid double serialization. Lambda will serialize the response.
9. Use the schema dynamically from a `schema` variable.
10. Validate 'table' against the unique table names in the schema (do not hardcode table names).
11. Include imports: json, boto3, datetime, logging.
12. Set up logger (INFO level) for event, validations, and DynamoDB operations.
13. Use 'row_key' as the DynamoDB partition key.

Schema:
{schema_json}

Supported Tables:
{tables_json}

Requirements:
- Use Python 3.9.
- Validation functions:
  - `validate_string(value, max_length, field_name)`: Check string type, max_length.
  - `validate_datetime(value, field_name)`: Validate ISO 8601 (YYYY-MM-DDThh:mm:ssZ).
- Transformation handling:
  - 'trim': Strip whitespace.
  - 'enum:Y,N': Check value in ['Y', 'N'].
  - 'require_one:email,phone_number': Validate at least one is non-empty.
  - 'require_if:first_name': Require if 'first_name' is non-empty.
- Keep code minimal (under 2000 characters) to avoid truncation.
- Ensure all try blocks have except clauses.
- No comments in the output code.

Example Code (MANDATORY):
import json
import boto3
import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')

schema = {schema_json}

def validate_string(value, max_length, field_name):
    if not isinstance(value, str):
        raise ValueError(f'{{field_name}} must be a string')
    if max_length != 'None' and len(value) > int(max_length):
        raise ValueError(f'{{field_name}} exceeds max length {{max_length}}')
    return value

def validate_datetime(value, field_name):
    try:
        datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        raise ValueError(f'{{field_name}} must be ISO 8601 (YYYY-MM-DDThh:mm:ssZ)')
    return value

def apply_transformations(value, transformation, field_name):
    if value is None:
        value = ''
    if isinstance(value, str):
        for t in transformation.split(';'):
            if t == 'trim':
                value = value.strip()
            elif t.startswith('enum:'):
                values = t.split(':')[1].split(',')
                if value not in values:
                    raise ValueError(f'{{field_name}} must be one of {{values}}')
    return value

def validate_conditional(data, schema, table):
    for field in schema:
        if field['table'] != table or field['required'] != 'conditional':
            continue
        transformations = field['transformation'].split(';')
        for t in transformations:
            if t.startswith('require_one:'):
                fields = t.split(':')[1].split(',')
                if not any(data.get(f, '').strip() for f in fields):
                    raise ValueError(f'One of {{fields}} is required')
            elif t.startswith('require_if:'):
                dep_field = t.split(':')[1]
                if data.get(dep_field, '').strip() and not data.get(field['api_field'], '').strip():
                    raise ValueError(f'{{field["api_field"]}} is required when {{dep_field}} is provided')

def build_nested_item(table, data, schema):
    item = {{}}
    profile = {{}}
    subscription = {{}}
    subscriptions = []
    for field in schema:
        if field['table'] != table:
            continue
        if field['api_field'].startswith('subscriptions[].'):
            sub_field = field['api_field'].split('.')[-1]
            for sub in data.get('subscriptions', []):
                value = sub.get(sub_field)
                if field['required'] == 'true' and (value is None or (isinstance(value, str) and not value.strip())):
                    raise ValueError(f'{{field["api_field"]}} is required')
                if value is not None:
                    value = apply_transformations(value, field['transformation'], field['api_field'])
                    if field['data_type'] == 'string':
                        value = validate_string(value, field['max_length'], field['api_field'])
                    elif field['data_type'] == 'datetime':
                        value = validate_datetime(value, field['api_field'])
                sub[sub_field] = value
            subscriptions = data.get('subscriptions', [])
        else:
            value = data.get(field['api_field'])
            if field['required'] == 'true' and (value is None or (isinstance(value, str) and not value.strip())):
                raise ValueError(f'{{field["api_field"]}} is required')
            if value is not None:
                value = apply_transformations(value, field['transformation'], field['api_field'])
                if field['data_type'] == 'string':
                    value = validate_string(value, field['max_length'], field['api_field'])
                elif field['data_type'] == 'datetime':
                    value = validate_datetime(value, field['api_field'])
            parts = field['dynamodb_column'].split(':')
            if parts[0] == 'profile':
                profile[parts[1]] = value
            elif parts[0] == 'subscription':
                subscription[parts[1]] = value
    if profile:
        item['profile'] = profile
    if subscription or subscriptions:
        subscription['subscriptions'] = subscriptions
        item['subscription'] = subscription
    validate_conditional(data, schema, table)
    return item

def lambda_handler(event, context):
    try:
        logger.info(f'Received event: {{json.dumps(event)}}')
        payload = event.get('body', event)
        try:
            if isinstance(payload, str):
                payload = json.loads(payload)
        except json.JSONDecodeError as e:
            logger.error(f'JSON error: {{e}}')
            return {{'statusCode': 400, 'body': {{'status': 'error', 'message': 'Invalid JSON'}}}}
        operations = payload.get('operations', [])
        if not operations:
            raise ValueError('Operations list required')
        results = []
        valid_tables = {{t['table'] for t in schema}}
        for op in operations:
            try:
                table = op.get('table')
                row_key = op.get('row_key')
                operation = op.get('operation')
                data = op.get('data', {{}})
                if not all([table, row_key, operation]):
                    raise ValueError('Missing table, row_key, or operation')
                if operation not in ['create', 'read', 'update', 'delete']:
                    raise ValueError(f'Invalid operation: {{operation}}')
                if table not in valid_tables:
                    raise ValueError(f'Unknown table: {{table}}')
                table_obj = dynamodb.Table(table)
                if operation in ['create', 'update']:
                    if not data:
                        raise ValueError('Data required')
                    item = build_nested_item(table, data, schema)
                    item['row_key'] = row_key
                    try:
                        table_obj.put_item(Item=item)
                        results.append({{'status': 'success', 'table': table, 'row_key': row_key, 'message': 'Data saved'}})
                    except Exception as e:
                        logger.error(f'DynamoDB write error for {{table}}/{{row_key}}: {{e}}')
                        results.append({{'status': 'error', 'table': table, 'row_key': row_key, 'message': str(e)}})
                elif operation == 'read':
                    try:
                        response = table_obj.get_item(Key={{'row_key': row_key}})
                        item = response.get('Item')
                        if not item:
                            raise ValueError(f'No item for row_key: {{row_key}}')
                        results.append({{'status': 'success', 'table': table, 'row_key': row_key, 'data': item}})
                    except Exception as e:
                        logger.error(f'DynamoDB read error for {{table}}/{{row_key}}: {{e}}')
                        results.append({{'status': 'error', 'table': table, 'row_key': row_key, 'message': str(e)}})
                elif operation == 'delete':
                    try:
                        response = table_obj.get_item(Key={{'row_key': row_key}})
                        if not response.get('Item'):
                            raise ValueError(f'No item for row_key: {{row_key}}')
                        table_obj.delete_item(Key={{'row_key': row_key}})
                        results.append({{'status': 'success', 'table': table, 'row_key': row_key, 'message': 'Data deleted'}})
                    except Exception as e:
                        logger.error(f'DynamoDB delete error for {{table}}/{{row_key}}: {{e}}')
                        results.append({{'status': 'error', 'table': table, 'row_key': row_key, 'message': str(e)}})
            except Exception as e:
                logger.error(f'Operation error for {{op}}: {{e}}')
                results.append({{'status': 'error', 'table': op.get('table', 'unknown'), 'row_key': op.get('row_key', 'unknown'), 'message': str(e)}})
        return {{'statusCode': 200, 'body': {{'status': 'success', 'results': results}}}}
    except Exception as e:
        logger.error(f'Error: {{e}}')
        return {{'statusCode': 500, 'body': {{'status': 'error', 'message': str(e)}}}}
"""

        bedrock_response = bedrock_client.invoke_model(
            modelId='amazon.nova-pro-v1:0',
            body=json.dumps({
                'messages': [
                    {
                        'role': 'user',
                        'content': [
                            {'text': prompt}
                        ]
                    }
                ]
            }),
            contentType='application/json',
            accept='application/json'
        )
        bedrock_output = json.loads(bedrock_response['body'].read().decode('utf-8'))
        logger.info(f"Bedrock response: {json.dumps(bedrock_output, indent=2)}")

        python_code = None
        if 'output' in bedrock_output and 'message' in bedrock_output['output'] and 'content' in bedrock_output['output']['message']:
            content = bedrock_output['output']['message']['content']
            if isinstance(content, list) and len(content) > 0 and 'text' in content[0]:
                python_code = content[0]['text']
        elif 'content' in bedrock_output and isinstance(bedrock_output['content'], list) and len(bedrock_output['content']) > 0:
            python_code = bedrock_output['content'][0].get('text')
        elif 'messages' in bedrock_output and isinstance(bedrock_output['messages'], list) and len(bedrock_output['messages']) > 0:
            message = bedrock_output['messages'][0]
            if 'content' in message and isinstance(message['content'], list) and len(message['content']) > 0:
                python_code = message['content'][0].get('text')
        elif 'output' in bedrock_output and 'text' in bedrock_output['output']:
            python_code = bedrock_output['output']['text']

        if not python_code:
            raise ValueError(f"Could not extract Python code from Bedrock response: {json.dumps(bedrock_output)}")

        python_code = clean_code(python_code)
        logger.info(f"Generated Python code (first 500 chars):\n{python_code[:500]}...")
        logger.info(f"Generated code length: {len(python_code)} chars")

        if not validate_code_syntax(python_code):
            logger.warning("Generated code invalid, using fallback")
            python_code = f"""
import json
import boto3
import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')

schema = {schema_json}

def validate_string(value, max_length, field_name):
    if not isinstance(value, str):
        raise ValueError(f'{{field_name}} must be a string')
    if max_length != 'None' and len(value) > int(max_length):
        raise ValueError(f'{{field_name}} exceeds max length {{max_length}}')
    return value

def validate_datetime(value, field_name):
    try:
        datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        raise ValueError(f'{{field_name}} must be ISO 8601 (YYYY-MM-DDThh:mm:ssZ)')
    return value

def apply_transformations(value, transformation, field_name):
    if value is None:
        value = ''
    if isinstance(value, str):
        for t in transformation.split(';'):
            if t == 'trim':
                value = value.strip()
            elif t.startswith('enum:'):
                values = t.split(':')[1].split(',')
                if value not in values:
                    raise ValueError(f'{{field_name}} must be one of {{values}}')
    return value

def validate_conditional(data, schema, table):
    for field in schema:
        if field['table'] != table or field['required'] != 'conditional':
            continue
        transformations = field['transformation'].split(';')
        for t in transformations:
            if t.startswith('require_one:'):
                fields = t.split(':')[1].split(',')
                if not any(data.get(f, '').strip() for f in fields):
                    raise ValueError(f'One of {{fields}} is required')
            elif t.startswith('require_if:'):
                dep_field = t.split(':')[1]
                if data.get(dep_field, '').strip() and not data.get(field['api_field'], '').strip():
                    raise ValueError(f'{{field["api_field"]}} is required when {{dep_field}} is provided')

def build_nested_item(table, data, schema):
    item = {{}}
    profile = {{}}
    subscription = {{}}
    subscriptions = []
    for field in schema:
        if field['table'] != table:
            continue
        if field['api_field'].startswith('subscriptions[].'):
            sub_field = field['api_field'].split('.')[-1]
            for sub in data.get('subscriptions', []):
                value = sub.get(sub_field)
                if field['required'] == 'true' and (value is None or (isinstance(value, str) and not value.strip())):
                    raise ValueError(f'{{field["api_field"]}} is required')
                if value is not None:
                    value = apply_transformations(value, field['transformation'], field['api_field'])
                    if field['data_type'] == 'string':
                        value = validate_string(value, field['max_length'], field['api_field'])
                    elif field['data_type'] == 'datetime':
                        value = validate_datetime(value, field['api_field'])
                sub[sub_field] = value
            subscriptions = data.get('subscriptions', [])
        else:
            value = data.get(field['api_field'])
            if field['required'] == 'true' and (value is None or (isinstance(value, str) and not value.strip())):
                raise ValueError(f'{{field["api_field"]}} is required')
            if value is not None:
                value = apply_transformations(value, field['transformation'], field['api_field'])
                if field['data_type'] == 'string':
                    value = validate_string(value, field['max_length'], field['api_field'])
                elif field['data_type'] == 'datetime':
                    value = validate_datetime(value, field['api_field'])
            parts = field['dynamodb_column'].split(':')
            if parts[0] == 'profile':
                profile[parts[1]] = value
            elif parts[0] == 'subscription':
                subscription[parts[1]] = value
    if profile:
        item['profile'] = profile
    if subscription or subscriptions:
        subscription['subscriptions'] = subscriptions
        item['subscription'] = subscription
    validate_conditional(data, schema, table)
    return item

def lambda_handler(event, context):
    try:
        logger.info(f'Received event: {{json.dumps(event)}}')
        payload = event.get('body', event)
        try:
            if isinstance(payload, str):
                payload = json.loads(payload)
        except json.JSONDecodeError as e:
            logger.error(f'JSON error: {{e}}')
            return {{'statusCode': 400, 'body': {{'status': 'error', 'message': 'Invalid JSON'}}}}
        operations = payload.get('operations', [])
        if not operations:
            raise ValueError('Operations list required')
        results = []
        valid_tables = {{t['table'] for t in schema}}
        for op in operations:
            try:
                table = op.get('table')
                row_key = op.get('row_key')
                operation = op.get('operation')
                data = op.get('data', {{}})
                if not all([table, row_key, operation]):
                    raise ValueError('Missing table, row_key, or operation')
                if operation not in ['create', 'read', 'update', 'delete']:
                    raise ValueError(f'Invalid operation: {{operation}}')
                if table not in valid_tables:
                    raise ValueError(f'Unknown table: {{table}}')
                table_obj = dynamodb.Table(table)
                if operation in ['create', 'update']:
                    if not data:
                        raise ValueError('Data required')
                    item = build_nested_item(table, data, schema)
                    item['row_key'] = row_key
                    try:
                        table_obj.put_item(Item=item)
                        results.append({{'status': 'success', 'table': table, 'row_key': row_key, 'message': 'Data saved'}})
                    except Exception as e:
                        logger.error(f'DynamoDB write error for {{table}}/{{row_key}}: {{e}}')
                        results.append({{'status': 'error', 'table': table, 'row_key': row_key, 'message': str(e)}})
                elif operation == 'read':
                    try:
                        response = table_obj.get_item(Key={{'row_key': row_key}})
                        item = response.get('Item')
                        if not item:
                            raise ValueError(f'No item for row_key: {{row_key}}')
                        results.append({{'status': 'success', 'table': table, 'row_key': row_key, 'data': item}})
                    except Exception as e:
                        logger.error(f'DynamoDB read error for {{table}}/{{row_key}}: {{e}}')
                        results.append({{'status': 'error', 'table': table, 'row_key': row_key, 'message': str(e)}})
                elif operation == 'delete':
                    try:
                        response = table_obj.get_item(Key={{'row_key': row_key}})
                        if not response.get('Item'):
                            raise ValueError(f'No item for row_key: {{row_key}}')
                        table_obj.delete_item(Key={{'row_key': row_key}})
                        results.append({{'status': 'success', 'table': table, 'row_key': row_key, 'message': 'Data deleted'}})
                    except Exception as e:
                        logger.error(f'DynamoDB delete error for {{table}}/{{row_key}}: {{e}}')
                        results.append({{'status': 'error', 'table': table, 'row_key': row_key, 'message': str(e)}})
            except Exception as e:
                logger.error(f'Operation error for {{op}}: {{e}}')
                results.append({{'status': 'error', 'table': op.get('table', 'unknown'), 'row_key': op.get('row_key', 'unknown'), 'message': str(e)}})
        return {{'statusCode': 200, 'body': {{'status': 'success', 'results': results}}}}
    except Exception as e:
        logger.error(f'Error: {{e}}')
        return {{'statusCode': 500, 'body': {{'status': 'error', 'message': str(e)}}}}
"""
            logger.info(f"Fallback code length: {len(python_code)} chars")
            if not validate_code_syntax(python_code):
                raise ValueError("Fallback code contains syntax errors")

        output_key = f"generated_code/{key.split('/')[-1].replace('.csv', '.py')}"
        s3_client.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=python_code.encode('utf-8')
        )
        logger.info(f"Saved Python code to s3://{bucket}/{output_key}")

        lambda_client.invoke(
            FunctionName='DeployerFunction',
            InvocationType='Event',
            Payload=json.dumps({
                'bucket': bucket,
                'key': output_key
            })
        )
        logger.info("Invoked DeployerFunction")

        return {
            'statusCode': 200,
            'body': json.dumps({'status': 'success', 'generated_file': output_key})
        }
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'status': 'error', 'message': str(e)})
        }