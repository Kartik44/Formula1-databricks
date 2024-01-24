# Databricks notebook source
# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC #Access azure data lake using Service Principle
# MAGIC ####1.Get client,tenent,secret values fro  key vault
# MAGIC ####2.set spark congfig for app/client_id/tenant_id/secrets.
# MAGIC ####4.Assign role"Storage Blob Data Contributor" to the data lake.
# MAGIC
# MAGIC ###5. Creating just  one scope is workale for all keys
# MAGIC
# MAGIC

# COMMAND ----------

def mount_adls(storage_account_name,container_name):
    #get secrets from key vaults 
    client_id=dbutils.secrets.get(scope='formula1-access-key-scope',key='formula2dl-client-id')
    tenant_id=dbutils.secrets.get(scope='formula1-access-key-scope',key='formula1dl-tenant-id')
    client_secret=dbutils.secrets.get(scope='formula1-access-key-scope',key='formula1dl-secret-value')

    #set spark config
    configs={"fs.azure.account.auth.type":"OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # unmount the mount point if it exists 
    if any(mount.mountPoint==f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    #Mount the storage account container
    dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point=f"/mnt/{storage_account_name}/{container_name}",
        extra_configs=configs)

    display(dbutils.fs.mounts())
         

# COMMAND ----------

mount_adls('formula1dl1999','raw')

# COMMAND ----------

mount_adls('formula1dl1999','processed')

# COMMAND ----------

mount_adls('formula1dl1999','presentation')

# COMMAND ----------


