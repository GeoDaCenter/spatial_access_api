import uuid
import os
import hashlib
from multiprocessing import JoinableQueue
import zipfile
import json

from Manifest import Manifest
from Consumer import Consumer

from ResourceManagerExceptions import ResourceDoesNotExistException


class ResourceManager:
    def __init__(self, num_processes=2, resource_lifespan=86400, job_lifespan=86400):
        self.allowed_extensions = {'csv'}
        self.resource_lifespan = resource_lifespan
        self.job_lifespan = job_lifespan
        self.num_processes = num_processes
        self.job_queue = None
        self.consumers = None
        self.manifest = Manifest()

    def start(self):
        self.job_queue = JoinableQueue()
        self.consumers = [Consumer(self.job_queue, self.manifest) for _ in range(self.num_processes)]
        for consumer in self.consumers:
            consumer.start()

    def shutdown(self):
        for consumer in self.consumers:
            consumer.terminate()

    def get_job_status(self, job_id):
        return self.manifest.get_job_status(job_id)

    def add_job_to_queue(self, job):
        self.job_queue.put(job)

    def delete_job_results(self, job_id):
        self.manifest.delete_job(job_id)
        path = 'jobs/' + job_id
        if os.path.exists(path):
            try:
                os.rmdir(path)
                return True
            except BaseException:
                return False
        return False

    def delete_expired_resources(self):
        expired_resources = self.manifest.delete_expired_resources(self.resource_lifespan)
        for resource_id in expired_resources:
            try:
                os.rmdir('resources/' + resource_id)
            except:
                pass

    def delete_expired_jobs(self):
        expired_jobs = self.manifest.delete_expired_jobs(self.job_lifespan)
        for job_id in expired_jobs:
            try:
                os.rmdir('jobs/' + job_id)
            except:
                pass

    @staticmethod
    def get_new_job_id():
        return uuid.uuid4().hex

    @staticmethod
    def get_zip_filename(job_id):
        folder = 'jobs/' + job_id + '/'
        if not os.path.exists(folder):
            return None
        zip_filename = job_id + '.zip'
        zip = zipfile.ZipFile(zip_filename, 'w')
        for file in os.listdir(folder):
            if not file.endswith('.zip'):
                zip.write(folder + file)
        return zip_filename

    @staticmethod
    def get_aggregated_data(job_id):
        filename = 'jobs/' + job_id + '/aggregate.json'
        if not os.path.exists(filename):
            return None
        with open(filename) as file:
            return json.load(file)

    @staticmethod
    def job_id_is_safe(job_id):
        return '/' not in job_id and '.' not in job_id

    @staticmethod
    def get_new_resource_id():
        return uuid.uuid4().hex

    @staticmethod
    def get_resource_hash(resource_id):
        filename = 'resources/' + resource_id
        if not os.path.exists(filename):
            return None
        block_size = 1024
        m = hashlib.sha256()
        with open(filename, "rb") as file:
            byte = file.read(block_size)
            while byte != b"":
                m.update(byte)
                byte = file.read(block_size)

        return m.hexdigest()

    def add_resource(self, resource_id):
        resource_hash = self.get_resource_hash(resource_id)
        self.manifest.add_resource(resource_id, resource_hash)

    def delete_resource(self, resource_id):
        self.manifest.delete_resource(resource_id)
        if not os.path.exists('resources/' + resource_id):
            raise ResourceDoesNotExistException(resource_id)
        os.remove('resources/' + resource_id)

    def extension_is_allowed(self, filename):
        if '.' not in filename:
            return False
        extension = filename.split('.')[1]
        return extension in self.allowed_extensions

    def resource_id_exists(self, resource_id):
        return self.manifest.resource_exists(resource_id)

    def resource_hash_exists(self, resource_hash):
        return self.manifest.resource_hash_exists(resource_hash)
