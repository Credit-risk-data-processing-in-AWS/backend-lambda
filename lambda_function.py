import json
import boto3
import time
import os
from decimal import Decimal

# Inicializar clientes de AWS
athena_client = boto3.client('athena')
s3_client = boto3.client('s3')

# Configuración
DATABASE = os.environ.get('GLUE_DATABASE', 'experian_risk_db')
TABLE = os.environ.get('GLUE_TABLE', 'risk_risk_profiles')
OUTPUT_BUCKET = os.environ.get('OUTPUT_BUCKET', 'experian-risk-data-geoffrey')
OUTPUT_PATH = f's3://{OUTPUT_BUCKET}/athena-results/'

class DecimalEncoder(json.JSONEncoder):
    """Helper para serializar Decimal a JSON"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def execute_athena_query(query):
    """
    Ejecuta una query en Athena y espera los resultados
    """
    try:
        # Iniciar ejecución de la query
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': DATABASE},
            ResultConfiguration={'OutputLocation': OUTPUT_PATH}
        )
        
        query_execution_id = response['QueryExecutionId']
        print(f"Query ejecutándose con ID: {query_execution_id}")
        
        # Esperar a que la query complete (máximo 30 segundos)
        max_attempts = 30
        attempt = 0
        
        while attempt < max_attempts:
            query_status = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            
            status = query_status['QueryExecution']['Status']['State']
            
            if status == 'SUCCEEDED':
                print("Query completada exitosamente")
                break
            elif status in ['FAILED', 'CANCELLED']:
                reason = query_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                raise Exception(f"Query falló con estado {status}: {reason}")
            
            time.sleep(1)
            attempt += 1
        
        if attempt >= max_attempts:
            raise Exception("Query timeout después de 30 segundos")
        
        # Obtener resultados
        results = athena_client.get_query_results(
            QueryExecutionId=query_execution_id,
            MaxResults=1000
        )
        
        return results
        
    except Exception as e:
        print(f"Error ejecutando query en Athena: {str(e)}")
        raise

def parse_athena_results(results):
    """
    Parsea los resultados de Athena a formato JSON
    """
    columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    rows = results['ResultSet']['Rows'][1:]  # Skip header row
    
    data = []
    for row in rows:
        values = [field.get('VarCharValue', '') for field in row['Data']]
        data.append(dict(zip(columns, values)))
    
    return data

def lambda_handler(event, context):
    """
    Handler principal de Lambda
    """
    print(f"Event recibido: {json.dumps(event)}")
    
    try:
        # Determinar el tipo de consulta
        http_method = event.get('httpMethod', 'GET')
        path = event.get('path', '/')
        query_params = event.get('queryStringParameters') or {}
        
        # Endpoint: GET /risk/{client_id}
        if http_method == 'GET' and path.startswith('/risk/'):
            client_id = path.split('/')[-1]
            
            query = f"""
            SELECT 
                client_id,
                total_debt,
                transaction_count,
                avg_transaction_amount,
                max_transaction_amount,
                min_transaction_amount,
                risk_level,
                risk_score,
                processing_date
            FROM {TABLE}
            WHERE client_id = '{client_id}'
            """
            
            results = execute_athena_query(query)
            data = parse_athena_results(results)
            
            if not data:
                return {
                    'statusCode': 404,
                    'headers': {
                        'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
                    })
                }
            
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
                },
                'body': json.dumps({
                    'success': True,
                    'data': data[0],
                    'source': 'Experian DataCrédito'
                }, cls=DecimalEncoder)
            }
        
        # Endpoint: GET /risk - Lista todos los clientes con paginación
        elif http_method == 'GET' and path == '/risk':
            risk_level_filter = query_params.get('risk_level', None)
            limit = int(query_params.get('limit', 100))
            
            where_clause = f"WHERE risk_level = '{risk_level_filter.upper()}'" if risk_level_filter else ""
            
            query = f"""
            SELECT 
                client_id,
                total_debt,
                transaction_count,
                risk_level,
                risk_score,
                processing_date
            FROM {TABLE}
            {where_clause}
            ORDER BY total_debt DESC
            LIMIT {limit}
            """
            
            results = execute_athena_query(query)
            data = parse_athena_results(results)
            
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
                },
                'body': json.dumps({
                    'success': True,
                    'count': len(data),
                    'data': data,
                    'filters': {'risk_level': risk_level_filter} if risk_level_filter else {},
                    'source': 'Experian DataCrédito'
                }, cls=DecimalEncoder)
            }
        
        # Endpoint: GET /stats - Estadísticas generales
        elif http_method == 'GET' and path == '/stats':
            query = f"""
            SELECT 
                risk_level,
                COUNT(*) as client_count,
                ROUND(AVG(total_debt), 2) as avg_debt,
                ROUND(SUM(total_debt), 2) as total_debt,
                MAX(processing_date) as last_update
            FROM {TABLE}
            GROUP BY risk_level
            ORDER BY risk_level
            """
            
            results = execute_athena_query(query)
            data = parse_athena_results(results)
            
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
                },
                'body': json.dumps({
                    'success': True,
                    'statistics': data,
                    'source': 'Experian DataCrédito'
                }, cls=DecimalEncoder)
            }
        
        # Endpoint: Healthcheck
        elif path == '/health':
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
                },
                'body': json.dumps({
                    'status': 'healthy',
                    'service': 'Experian Risk API',
                    'version': '1.0',
                    'database': DATABASE,
                    'table': TABLE
                })
            }
        
        # Ruta no encontrada
        # Endpoint OPTIONS para preflight CORS
        elif http_method == 'OPTIONS':
            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
                },
                'body': ''
            }
        
        # Ruta no encontrada
        else:
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
                },
                'body': json.dumps({
                    'error': 'Endpoint no encontrado',
                    'path': path,
                    'method': http_method,
                    'available_endpoints': [
                        'GET /health',
                        'GET /risk',
                        'GET /risk/{client_id}',
                        'GET /stats'
                    ]
                })
            }
    
    except Exception as e:
        print(f"Error en Lambda: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
            },
            'body': json.dumps({
                'error': 'Error interno del servidor',
                'message': str(e)
            })
        }
