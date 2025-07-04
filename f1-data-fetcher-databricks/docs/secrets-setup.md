

## 1. Create a Secret Scope

Use the Databricks CLI:

bash
databricks secrets create-scope --scope f1_scope


databricks secrets put --scope f1_scope --key openf1_api_key
databricks secrets put --scope f1_scope --key mongo_uri


dbutils.secrets.get(scope="f1_scope", key="openf1_api_key")
dbutils.secrets.get(scope="f1_scope", key="mongo_uri")
