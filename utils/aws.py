__AUTHOR__ = "Soumil shah "
__EMAIL__ = "shahsoumil519@gmail.com"


try:
    import sys
    import importlib.util
    from abc import ABC, abstractmethod
    import boto3
    import json
    print("All Modules ok .....   ")
except Exception as e:
    print("Error :{} ".format(e))


global AWS_ACCESS_KEY
global AWS_SECRET_KEY
global AWS_REGION_NAME

AWS_ACCESS_KEY = "XXX"
AWS_SECRET_KEY ="XXX"
AWS_REGION_NAME = "us-east-1"


class AWSS3(object):

    """Helper class to which add functionality on top of boto3 """

    def __init__(self, bucket='soumilshahdevbucket'):
        self.BucketName = bucket
        self.client = boto3.client("s3",
                                   aws_access_key_id=AWS_ACCESS_KEY,
                                   aws_secret_access_key=AWS_SECRET_KEY,
                                   region_name=AWS_REGION_NAME)

    def put_files(self, Response=None, Key=None):
        """
        Put the File on S3
        :return: Bool
        """
        try:

            DATA = bytes(json.dumps(Response).encode('UTF-8'))

            response = self.client.put_object(
                ACL='private',
                Body=DATA,
                Bucket = self.BucketName,
                Key=Key
            )
            return 'ok'
        except Exception as e:
            print("Error : {} ".format(e))
            return 'error'

    def item_exists(self, Key):
        """Given key check if the items exists on AWS S3 """
        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return True
        except Exception as e:
            return False

    def get_item(self, Key):

        """Gets the Bytes Data from AWS S3 """

        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return response_new["Body"].read()
        except Exception as e:
            print("Error", e)
            return False

    def find_one_update(self, data=None, key=None):

        """
        This checks if Key is on S3 if it is return the data from s3
        else store on s3 and return it
        """

        flag = self.item_exists(Key=key)

        if flag:
            data = self.get_item(Key=key)
            return data

        else:
            self.put_files(Key=key, Response=data)
            return data

    def get_all_keys(self ,Prefix=''):
        """
        :param Prefix: Prefix string
        :return: Keys List
        """

        paginator = self.client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.BucketName, Prefix=Prefix)

        tmp = []

        for page in pages:
            for obj in page['Contents']:
                tmp.append(obj['Key'])

        return tmp

    def print_tree(self):
        keys = self.get_all_keys()
        for key in keys:
            print(key)
        return None

    def find_one_similar_key(self, searchTerm=''):
        keys = self.get_all_keys()
        return [key for key in keys if re.search(searchTerm, key)]

    def __repr__(self):
        return "AWS S3 Helper class "


class LoaderInterface(ABC):

    @abstractmethod
    def get_instance(self):
        """Fetch the Scrappers from DB """
        pass


class Loaderinterface(ABC):

    @abstractmethod
    def get_instance(self):
        pass


class Loader(Loaderinterface, AWSS3):

    def __init__(self, Key='moda.py'):
        self.Key = Key
        AWSS3.__init__(self)

    def get_instance(self):

        response = self.get_item(Key=self.Key)
        my_name = 'my_module'
        my_spec = importlib.util.spec_from_loader(my_name, loader=None)
        my_module = importlib.util.module_from_spec(my_spec)
        exec(response, my_module.__dict__)

        return my_module


helper = Loader(Key='moda.py')
instance = helper.get_instance()
print(instance.Foo())