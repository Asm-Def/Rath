import json
from sqlalchemy import create_engine
import base64


class basefunc:
    # bigquery
    @staticmethod
    def bigquery_getdb(uri, schema, credentials):
        project_id = uri.split(r'//')[1]
        engine = create_engine(uri, credentials_base64=credentials, echo=True)
        res = engine.execute('SELECT schema_name FROM {0}.INFORMATION_SCHEMA.SCHEMATA'.format(project_id)).fetchall()
        db_list = []
        for row in res:
            for item in row:
                db_list.append(item)
        return db_list

    @staticmethod
    def bigquery_gettable(uri, database, schema, credentials):
        engine = create_engine(uri, credentials_base64=credentials, echo=True)
        res = engine.execute('SELECT table_name FROM {0}.INFORMATION_SCHEMA.TABLES'.format(database)).fetchall()
        table_list = []
        for row in res:
            for item in row:
                meta = basefunc.bigquery_getmeta(database=database, schema=schema, table=item, engine=engine)
                scores = {"name": item, "meta": meta}
                table_list.append(scores)
        return table_list

    @staticmethod
    def bigquery_getmeta(database, table, schema, engine=None):
        meta_res = engine.execute('''
        SELECT
          * 
        FROM
          {0}.INFORMATION_SCHEMA.COLUMNS
        WHERE
          table_name = "{1}"'''.format(database, table)).fetchall()
        meta = []
        i = 0
        for col_data in meta_res:
            scores = {"key": col_data.column_name, "colIndex": i, "dataType": col_data.data_type}
            meta.append(scores)
            i += 1
        return meta

    @staticmethod
    def bigquery_getdata(uri, database, table, schema, rows_num, credentials):
        engine = create_engine(uri, credentials_base64=credentials, echo=True)
        data_res = engine.execute('select * from ' + database + '.' + table + ' limit ' + rows_num).fetchall()
        data = []
        for row in data_res:
            rows = []
            for item in row:
                rows.append(item)
            data.append(rows)
        return data

    @staticmethod
    def bigquery_getdetail(uri, database, table, schema, rows_num, credentials):
        engine = create_engine(uri, credentials_base64=credentials, echo=True)
        meta = basefunc.bigquery_getmeta(database=database, schema=schema, table=table, engine=engine)
        sql = f'select * from {database}.{table} limit {rows_num}'
        res_list = basefunc.bigquery_getresult(sql=sql, engine=engine)
        return [meta, res_list[0], res_list[1]]

    @staticmethod
    def bigquery_getresult(sql, credentials=None, uri=None, engine=None):
        if engine is None:
            engine = create_engine(uri, credentials_base64=credentials, echo=True)
        res = engine.execute(sql)
        data_res = res.fetchall()
        col_res = res.keys()
        sql_result = []
        for row in data_res:
            rows = []
            for item in row:
                rows.append(item)
            sql_result.append(rows)
        columns = []
        for col_data in col_res:
            columns.append(col_data)
        return [columns, sql_result]


def lambda_handler(event, context):
    uri = event['uri']
    source_type = event['sourceType']
    func = event['func']
    database = event['db']
    table = event['table']
    schema = event['schema']
    rows_num = event['rowsNum']
    sql = event['query']
    credentials = json.dumps(event['credentials'])
    credentials_64 = base64.b64encode(credentials.encode('utf-8'))
    dict_func = basefunc.__dict__
    if func == 'getDatabases':
        db_list = dict_func['{0}_getdb'.format(source_type)].__func__(uri=uri, schema=schema,
                                                                      credentials=credentials_64)
        return db_list
    elif func == 'getSchemas':
        schema_list = dict_func['{0}_getschema'.format(source_type)].__func__(uri=uri, db=database,
                                                                              credentials=credentials_64)
        return schema_list
    elif func == 'getTables':
        table_list = dict_func['{0}_gettable'.format(source_type)].__func__(uri=uri, database=database, schema=schema,
                                                                            credentials=credentials_64)
        return table_list
    elif func == 'getTableDetail':
        res_list = dict_func['{0}_getdetail'.format(source_type)].__func__(uri=uri, database=database, table=table,
                                                                           schema=schema, rows_num=rows_num,
                                                                           credentials=credentials_64)
        return {
            "meta": res_list[0],
            "columns": res_list[1],
            "rows": res_list[2]
        }
    elif func == 'getResult':
        res_list = dict_func['{0}_getresult'.format(source_type)].__func__(uri=uri, sql=sql,
                                                                           credentials=credentials_64)
        return {
            "columns": res_list[0],
            "rows": res_list[1]
        }
    else:
        return 'The wrong func was entered'


if __name__ == '__main__':
    events = {
        "uri": "bigquery://kanaries-demo",
        "sourceType": "bigquery",
        "func": "getTableDetail",
        "db": "demo",
        "table": "students",
        "schema": "value3",
        "rowsNum": "3",
        "query": "select * from demo.students limit 3",
        "credentials": {
            "type": "service_account",
            "project_id": "kanaries-demo",
            "private_key_id": "f4bb54391d7e1d7b54e8d5d96bbe5b59663d8c8b",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDXks+zkc2zqHJC\n/XU30KkzdNDwU+bXwkBNImOn+MYXWboOYe09l6HHKC49AJmnSCG72wCxjvAzwU5V\nFSObRA6botLNFnBYQXb3WzSrOXSXomU6S1z7+ypQrDZI7QhwqxM1toKC5Tbuv4A4\namnZttFiAFYmx4AAUUhVgkxPFqNQGgw70dWYXYSsr8pYBAGufpZqglyu4rUDJ/oG\n10XuMKHmBfR5nN3CFtCvSqXijPWbWmrk5pG+nGvMAFXNCpq4eXnABWTQcjK6Mbl0\nzEsZgSfEWAIrXnDrTela0K6uXQxUHX4ZwTv1V3sByPKkFQWYPiTTzew2qMmHUAkz\nFeLptxsZAgMBAAECggEAIZ86uNGVSR+NDqi0Vwu60BU4nzsexmj8GWuzKlgRIUQz\n0hlw5InZSBQavhXxYRdNd5ytK4RVL2VHX9rHrmg7dQe8pBMLW6B+Ow8lFE2GQz4n\nVqO5cW2XiHSLlO0vQ09TVC3OhjbUzgDdMS9bqgKq/oN57FtyOdpzky6a0zz/JLdZ\nyv8H4TTkF5dzzRT+/lOLLWgDaVSVxQvdG0KgqB7ull/VXvzgi2Jpk5mZxvBsAZwc\nuod/b07IFv2NWAg72FmSLIR7GG9jMmIfysbtr9mxsvLCoujcztlUw3GilcKsg0Jd\nHyDvS3i33QDaAc+xaZkP/m5btMGNydoueKE7e+stfwKBgQD+vQQcsVe6gXDgCGLd\n4HbdN4jiPkKKnsQarRxnXsYMHJ0johcIVUlJM5UHPX79xnaSPns8KG27F0cOLStX\nQbjzPhYFGmD385WG5D56eiZWUph5yAJ2EBZ3U1xTa7PwH34VqGAkwTA7e205jnF/\nj7KKaY6PqMBBUBJmwcvG4rqVtwKBgQDYpCNRSMerhsj3pZPJ8mhPcmJihpC8Nv4F\nqqhmjLjFakbsZWkRg7hnkBEZvbtOwBZENA6XuTcbp6WF8o6bvwwZTt0pxA41VY91\nJmGy2WZs5YiLNsdeVKuG/2R6IZtds1ebMoBy/zUTxxt+gxtFla0JSl5dNxUcjHII\nm1kPAzNVrwKBgC+9SJn/+krvmzHBIJYoTN5kW/jaZioIWwQM0TfmIQOAEUruQ5bC\nNPvM+O7kbXotyWba4smBYh8f26cie+7cWEbtqb7HFMkjEzC2cacOYUToMb8Q5rUt\niqhOLQ1NL+meXUi9x1bcBagAF5Yjxc18Jp+d7KOromwbD3fGdeQN9Z29AoGBANVZ\nSAK71w+gDHh+kr31wS5Eaom3FgCVc7Lm7zMW8LxSPoh+EmwSOV+cLIsaI8WZPBRs\n49YdrBzLDCKOzkypZ+Pgm8OO9aMmoMHxS5PTr6AcqEzZZJwMbGPlTfFM+XHctbD8\nmi0mEgJpOjVsLkZn8eHNYIMSRPM5iO/a9WjocPy3AoGBAKh35xKGhoHr3EC1YpxR\nyeFlnWKI1hKliXD+924Ar6sxhzTZtQ2rmalYfZE9N+fgGhQrwlWQrcVmLA2IkY34\nTSobaQSYfTlromjJJUSLXAkqw3tUXzvA9hIJ9qSmTUfxD8YgKrqsOyDDJFhrf1UF\no5t60GqsNYs/sxSF18HILfnT\n-----END PRIVATE KEY-----\n",
            "client_email": "kanaries-bigquery-demo@kanaries-demo.iam.gserviceaccount.com",
            "client_id": "117503194581204202733",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/kanaries-bigquery-demo%40kanaries-demo.iam.gserviceaccount.com"
        }
    }
    context = None
    print(lambda_handler(events, context))
