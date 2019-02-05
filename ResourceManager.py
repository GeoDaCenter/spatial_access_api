import uuid
import os
import json
import hashlib
import time
import threading

from spatial_access.p2p import TransitMatrix
from spatial_access.CommunityAnalytics import DestFloatingCatchmentArea
from spatial_access.CommunityAnalytics import TwoStageFloatingCatchmentArea
from spatial_access.CommunityAnalytics import AccessTime
from spatial_access.CommunityAnalytics import AccessCount
from spatial_access.CommunityAnalytics import AccessModel
from spatial_access.SpatialAccessExceptions import *


class ResourceDoesNotExistException(BaseException):
    """Resource does not exist"""
    pass


class ManifestDoesNotExistException(BaseException):
    """Manifest does not exist"""
    pass


class UnknownJobTypeException(BaseException):
    """Unknown job type"""
    def __repr__(self):
        return "Unknown job type"




class ResourceManager:
    def __init__(self):
        self.allowed_extensions = {'csv', 'png'}
        self.resource_lifespan = 60 * 60 * 24  # one day
        self.job_queue = []
        self.sleep_interval = 1
        self.current_job = None
        thread = threading.Thread(target=self.run_job_queue, args=())
        thread.daemon = True  # Daemonize thread
        thread.start()  # Start the execution

    def dispatch_job(self, job):
        print('job:', job)
        if not os.path.exists('jobs/'):
            os.mkdir('jobs/')
        os.mkdir('jobs/' + job['job_id'])
        if job['job_type'] == 'model':
            try:
                print('model job')
            except Exception as exception:
                job['exception'] = str(exception)
        elif job['job_type'] == 'matrix':
            try:
                print('matrix job')
            except Exception as exception:
                job['exception'] = exception
        else:
            job['exception'] = str(UnknownJobTypeException(job))
        self.write_job(job)

    def write_job(self, job):
        manifest = self.get_manifest()
        manifest['jobs'][job['job_id']] = job

        self.write_manifest(manifest)

    def get_job_status(self, job_id):
        if self.current_job and job_id == self.current_job['job_id']:
            return 'current'
        elif job_id in self.job_queue:
            return 'enqueued'
        else:
            manifest = self.get_manifest()
            if job_id in manifest['jobs'].keys():
                if 'exception' in manifest['jobs'][job_id].keys():
                    return 'encountered_exception'
                return 'finished'
            return 'not_running'

    def get_job_results(self, job_id):
        manifest = self.get_manifest()
        if job_id in manifest['jobs'].keys():
            return manifest['jobs'][job_id]
        return None

    def add_job_to_queue(self, job):
        self.job_queue.append(job)

    def cancel_job(self, job_id):
        index_to_cancel = None
        for i, job in enumerate(self.job_queue):
            if job['job_id'] == job_id:
                index_to_cancel = i
                break
        if index_to_cancel:
            self.job_queue.pop(index_to_cancel)
            return True
        return False

    def delete_job_results(self, job_id):
        manifest = self.get_manifest()
        if job_id in manifest['jobs'].keys():
            os.rmdir('jobs/' + job_id)
            del manifest['jobs'][job_id]
            self.write_manifest(manifest)
            return True
        return False

    def run_job_queue(self):
        while True:
            if len(self.job_queue) > 0:
                self.current_job = self.job_queue.pop(0)
                self.dispatch_job(self.current_job)
                self.current_job = None
            else:
                time.sleep(self.sleep_interval)

    @staticmethod
    def get_manifest():
        if os.path.exists('manifest.json'):
            with open('manifest.json', 'r') as file:
                return json.load(file)
        else:
            return {'resources':{}, 'jobs':{}}

    @staticmethod
    def write_manifest(manifest):
        with open('manifest.json', 'w') as file:
            return json.dump(manifest, file)

    @staticmethod
    def get_new_job_id():
        return uuid.uuid4().hex

    @staticmethod
    def get_new_resource_id():
        return uuid.uuid4().hex

    @staticmethod
    def get_resource_hash(resource_id):
        filename = 'resources/' + resource_id
        block_size = 1024
        m = hashlib.sha256()
        with open(filename, "rb") as file:
            byte = file.read(block_size)
            while byte != b"":
                m.update(byte)
                byte = file.read(block_size)

        return m.hexdigest()

    def add_resource(self, resource_id):
        manifest = self.get_manifest()
        resource_hash = self.get_resource_hash(resource_id)
        manifest['resources'][resource_id] = {'hash': resource_hash, 'timestamp': time.time()}
        self.write_manifest(manifest)

    def delete_resource(self, resource_id):
        manifest = self.get_manifest()
        if resource_id not in manifest['resources'].keys():
            raise ResourceDoesNotExistException(resource_id)
        del manifest['resources'][resource_id]
        self.write_manifest(manifest)
        try:
            os.remove('resources/' + resource_id)
        except FileNotFoundError:
            raise ResourceDoesNotExistException(resource_id)

    def extension_is_allowed(self, filename):
        if '.' not in filename:
            return False
        extension = filename.split('.')[1]
        return extension in self.allowed_extensions

    def resource_id_exists(self, resource_id):
        manifest = self.get_manifest()
        return resource_id in manifest['resources'].keys()

    def resource_hash_exists(self, resource_hash):
        manifest = self.get_manifest()
        for resource_id, resource in manifest['resources'].items():
            if resource['hash'] == resource_hash:
                return resource_id
        return None

    def delete_expired_resources(self):
        resources_to_delete = []
        manifest = self.get_manifest()
        for resource_id, resource_details in manifest['resources'].items():
            if time.time() - resource_details['timestamp'] > self.resource_lifespan:
                resources_to_delete.append(resource_id)

        for resource_id in resources_to_delete:
            self.delete_resource(resource_id)