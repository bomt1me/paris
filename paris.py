import abc
import queue
import threading
import time
import uuid

import boto3


class AbstractConfig(abc.ABC):
    @abc.abstractmethod
    def get(self, key):
        raise NotImplementedError

    @abc.abstractmethod
    def put(self, key, value):
        raise NotImplementedError


class PropertiesConfig(AbstractConfig):
    def __init__(self):
        self._conf = {}

    def get(self, key):
        return self._conf.get(key, None)

    def put(self, key, value):
        self._conf[str(key)] = str(value)

    @classmethod
    def from_properties(cls, properties):
        conf = PropertiesConfig()
        for prop in properties:
            conf.put(str(prop[0]), str(prop[1]))

        return conf


class File:
    def __init__(self, config, filepath, filetype, filename):
        self._config = config
        self._filepath = filepath
        self._filetype = filetype
        self._filename = filename

    @property
    def bucket(self):
        confkey = f"files.{str(self._filetype)}.bucket"
        return self._config.get(confkey)

    @property
    def key(self):
        confkey = f"files.{str(self._filetype)}.key"
        prefix = self._config.get(confkey)
        if prefix:
            return f"{prefix}{self._filename}"
        return self._filename

    @property
    def filepath(self):
        return self._filepath


class StorageEngine(abc.ABC):
    @property
    @abc.abstractmethod
    def identifier(self):
        raise NotImplementedError

    @abc.abstractmethod
    def upload_file(self, file):
        raise NotImplementedError

    @abc.abstractmethod
    def open(self):
        raise NotImplementedError

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError


class SimpleStorage(StorageEngine):
    def __init__(self, config, client=None):
        self._config = config
        self._client = client
        self._identifier = "s3"

    @property
    def identifier(self):
        return self._identifier

    def upload_file(self, file):
        self._client.upload_file(
            Filename=file.filepath,
            Bucket=file.bucket,
            Key=file.key,
        )

    def open(self):
        self.close()
        self._client = boto3.client(
            "s3",
            aws_access_key_id=self._config.get("aws_access_key_id"),
            aws_secret_access_key=self._config.get("aws_secret_access_key"),
            endpoint_url=self._config.get("endpoint_url"),
        )

    def close(self):
        if self._client:
            self._client.close()
            self._client = None


class StorageFactory:
    def __init__(self, config, storage_options=None):
        self._config = config
        self._options = storage_options or {}

    def register(self, storage_engine):
        self._options[storage_engine.identifier] = storage_engine

    def build(self):
        storage_type = self._config.get("files.provider")
        if option := self._options.get(storage_type):
            option.open()
            return option

        raise NotImplementedError


class Worker:
    def __init__(
        self,
        config,
        worker_queue,
        cancellation_token,
        factory,
    ):
        self._config = config
        self._worker_queue = worker_queue
        self._cancellation_token = cancellation_token
        self._factory = factory
        self._uploader = factory.build()
        self._timeout = float(self._config.get("worker.timeout") or 0.2)

    def run(self):
        while not self._cancellation_token.is_set():
            try:
                item = self._worker_queue.get(block=False)
            except queue.Empty:
                item = None

            if item:
                self._uploader.upload_file(item)

            time.sleep(self._timeout)


class Manager:
    def __init__(
        self,
        config,
        worker_queue,
        cancellation_token,
        factory,
    ):
        self._config = config
        self._worker_queue = worker_queue
        self._cancellation_token = cancellation_token
        self._factory = factory
        self._workers = []
        self._number_of_threads = int(self._config.get("worker.threads") or 1)
        self._join_timeout = int(self._config.get("worker.join.timeout") or 60)

    def prepare(self):
        self._workers.clear()
        for _ in range(0, self._number_of_threads):
            worker = Worker(
                self._config, self._worker_queue, self._cancellation_token, self._factory
            )
            worker_thread = threading.Thread(target=worker.run)
            self._workers.append(worker_thread)

    def start(self):
        for worker_thread in self._workers:
            worker_thread.start()

    def stop(self):
        for worker_thread in self._workers:
            worker_thread.join(timeout=self._join_timeout)


if __name__ == "__main__":
    configuration = PropertiesConfig.from_properties(
        [
            ["aws_access_key_id", "miniobomt1me"],
            ["aws_secret_access_key", "miniobomt1me"],
            ["endpoint_url", "http://localhost:9000"],
            ["files.upload.bucket", "bom"],
            ["files.upload.key", "upload/"],
            ["files.provider", "s3"],
            ["worker.threads", "7"],
            ["worker.join.timeout", "60"],
            ["worker.timeout", "0.05"],
        ]
    )
    storage_factory = StorageFactory(configuration)
    storage = SimpleStorage(configuration)
    file_upload_queue = queue.Queue()
    file_upload_cancellation_token = threading.Event()
    storage_factory.register(storage)
    manager = Manager(
        configuration, file_upload_queue, file_upload_cancellation_token, storage_factory
    )
    manager.prepare()
    manager.start()

    for idx in range(0, 1000):
        file_upload_queue.put(
            File(
                configuration,
                "./carne_asada.dat",
                "upload",
                f"{idx}_carne_asada_{uuid.uuid4()}.dat",
            )
        )

    time.sleep(60)
    file_upload_cancellation_token.set()

    manager.stop()
