from hdfs import InsecureClient
from hdfs.ext.kerberos import KerberosClient
import os, pathlib, string, random

class HadoopStorage:
    def __init__(self):
        self.hadoop_host = os.getenv('HADOOP_HOST')
        self.hadoop_port = os.getenv('HADOOP_PORT')
        self.hadoop_user = os.getenv('HADOOP_USER')
        self.hadoop_secure = int(os.getenv('HADOOP_SECURE', default=0))

        if self.hadoop_secure:
            self.client = KerberosClient(f'http://{self.hadoop_host}:{self.hadoop_port}')
        else:
            self.client = InsecureClient(f'http://{self.hadoop_host}:{self.hadoop_port}', user=self.hadoop_user)

    # def _open(self, name, mode='rb'):
    #   pass

    def _save(self, name, content):
        # Save the file to HDFS. Not to be used directly. Will overwrite file if same filename exists
        with self.client.write(name, overwrite=True) as writer:
            writer.write(content.read())

        return name
    

    def delete(self, name):
        # deletes the file from HDFS
        print("name=",name)
        self.client.delete(name)


    def exists(self, name):
        # return True if file already exists, false if available
        name = str(name).replace("\\", "/")
        dir_name, file_name = os.path.split(name)

        if not dir_name:
            dir_name = '.'

        return (file_name in self.client.list(dir_name))


    def listdir(self, path):
        # list contents of specified path
        return (self.client.list(path))


    def size(self, name):
        # return total size in bytes
        return self.client.content(name)['spaceConsumed']


    def url(self, name):
        # return url where contents of file can be accessed
        return f'http://{self.hadoop_host}:{self.hadoop_port}/webhdfs/v1/user/{self.hadoop_user}/{name}?op=OPEN' # Need to append authentication query param when rendering on client side, either user.name for simple auth, or delegation token for kerberos


    def save(self, name, content, max_length=None):
        """
        Save new content to the file specified by name. The content should be
        a Python file-like object, ready to be read from the beginning.
        """
        if name is None:
            name = content.name

        name = self.get_available_name(name, max_length=max_length)
        name = self._save(name, content)

        return name


    def get_alternative_name(self, file_root, file_ext):
        """
        Return an alternative filename, by adding an underscore and a random 7
        character alphanumeric string (before the file extension, if one
        exists) to the filename.
        """
        N = 7
        res = ''.join(random.choices(string.ascii_uppercase + string.digits, k=N))
        
        return "%s_%s%s" % (file_root, str(res), file_ext)


    def get_available_name(self, name, max_length=None):
        """
        Return a filename that's free on the target storage system and
        available for new content to be written to.
        """
        name = str(name).replace("\\", "/")
        dir_name, file_name = os.path.split(name)

        if ".." in pathlib.PurePath(dir_name).parts:
            raise Exception("Detected path traversal attempt in '%s'" % dir_name)
        
        file_root, file_ext = os.path.splitext(file_name)

        # If the filename already exists, generate an alternative filename
        # until it doesn't exist.
        # Truncate original name if required, so the new filename does not
        # exceed the max_length.

        while self.exists(name) or (max_length and len(name) > max_length):
            # file_ext includes the dot.
            name = os.path.join(
                dir_name, self.get_alternative_name(file_root, file_ext)
            )
            if max_length is None:
                continue
            # Truncate file_root if max_length exceeded.
            truncation = len(name) - max_length
            if truncation > 0:
                file_root = file_root[:-truncation]
                # Entire file_root was truncated in attempt to find an
                # available filename.
                if not file_root:
                    raise Exception(
                        'Storage can not find an available filename for "%s". '
                        "Please make sure that the corresponding file field "
                        'allows sufficient "max_length".' % name
                    )
                name = os.path.join(
                    dir_name, self.get_alternative_name(file_root, file_ext)
                )
        return name




