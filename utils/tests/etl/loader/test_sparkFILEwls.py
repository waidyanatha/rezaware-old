import os
import sys

def init_classes(root):

    try:
        sys.path.insert(1,root)
        import rezaware as reza
        from utils.modules.etl.loader import sparkFILEwls as spark
        from utils.modules.etl.loader.sparkFILEwls import credentials as cred

        __desc__ = "read and write files from and to a particular source"
        clsSpark = spark.FileWorkLoads(desc=__desc__)
        if clsSpark._session:
            clsSpark._session.stop

        return None

    except Exception as err:
#         logger.error("%s %s \n",__s_fn_id__, err)
#         logger.debug(traceback.format_exc())
#         print("[Error]"+__s_fn_id__, err)

        return err

def test_init():
    root = "/home/nuwan/workspace/rezaware/"
    assert init_classes(root) == None

def test_read_file():
    clsSpark.storeMode = "local-fs"
    assert clsSpark.storeMode == "local-fs"
#     # clsSpark.storeMode = "aws-s3-bucket"
#     # clsSpark.storeMode = "google-storage"
#     print("mode =",clsSpark.storeMode)

#     ''' set the driver '''
#     if clsSpark.storeMode.lower() == "local-fs":
#         clsSpark.jarDir = "/opt/spark/jars/postgresql-42.5.0.jar"
#     elif clsSpark.storeMode.lower() == "aws-s3-bucket":
#         clsSpark.jarDir = "/opt/spark/jars/aws-java-sdk-s3-1.12.376.jar"
#     elif clsSpark.storeMode.lower() == "google-storage":
#         clsSpark.jarDir = "/opt/spark/jars/gcs-connector-hadoop3-2.2.10.jar"
#     else:
#         pass
#     print("jar =",clsSpark.jarDir)