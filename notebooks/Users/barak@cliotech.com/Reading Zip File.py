# Databricks notebook source
# Mount Azure Datastore
storage_account_name = "dataloadestore"
storage_account_access_key = "7wtLQIcK9q4QnXMCL6AO9I233TSi3hITG6tC4jO5VDEv3+ovoQo6NYv5IcboZo6Ncf5GeULV7uPdvUW+k8gJGA=="
container = "rawdata"
folder = "platformusage_events/"

# print (storage_account_name+storage_account_access_key ) 

# COMMAND ----------


dbutils.fs.mount(
  source = "wasbs://" + container + "@" + storage_account_name +".blob.core.windows.net/" + folder ,
  mount_point = "/mnt/"+ folder ,
  extra_configs = {"fs.azure.account.key." + storage_account_name + ".blob.core.windows.net":storage_account_access_key})

#extra_configs = {"fs.azure.account.key.<myaccount>.blob.core.chinacloudapi.cn": "ufU474A47XXXXXXXXXXXXXXXXXXXXXXXXXXZZU0LZmxQL5P1vLDH8FbGcdDGCVWX2cIGR"} 


# COMMAND ----------

dbutils.fs.ls("/mnt/platformusage_events/20180801/")
# dbutils.fs.rm("/mnt/rawdata")
# dbutils.fs.unmount("/mnt/rawdata/")

# COMMAND ----------

 textFile = spark.read.text("dbfs:/mnt/platformusage_events/20180801/20180801.zip")

# COMMAND ----------

textFile.first()

# COMMAND ----------

import zipfile
filepath = "dbfs:/mnt/platformusage_events/20180801/20180801.zip"

# COMMAND ----------



for filename in [ filepath, 'example.zip', 'bad_example.zip', 'notthere.zip' ]:
    print  (filename, zipfile.is_zipfile(filename))

# COMMAND ----------

zfile = zipfile.ZipFile(filepath)

# COMMAND ----------

for finfo in zfile.infolist():
        ifile = zfile.open(finfo)
        line_list = ifile.readlines()
        print line_list

# COMMAND ----------

import zipfile

def read_zip_file(filepath):
    zfile = zipfile.ZipFile(filepath)
    for finfo in zfile.infolist():
        ifile = zfile.open(finfo)
        line_list = ifile.readlines()
        print line_list

# COMMAND ----------

1

# COMMAND ----------

folder

# COMMAND ----------

