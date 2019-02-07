from multiprocessing import Lock
import json
import time
import os

class Manifest:
    def __init__(self):
        self.lock = Lock()

    def _load(self):
        if os.path.exists('manifest.json'):
            with open('manifest.json', 'r') as file:
                self.lock.acquire()
                try:
                    manifest = json.load(file)
                except:
                    manifest = {'resources':{},
                    'jobs':{}}
                self.lock.release()
                return manifest
        else:
            return {'resources':{},
                    'jobs':{}}

    def _write(self, manifest):
        with open('manifest.json', 'w') as file:
            self.lock.acquire()
            try:
                json.dump(manifest, file)
            except:
                pass
            self.lock.release()

    def clear(self):
        os.remove('manifest.json')

    def add_resource(self, resource_id, resource_hash):
        manifest = self._load()

        manifest['resources'][resource_id] = {'hash':resource_hash,
                                              'timestamp':time.time()}
        self._write(manifest)

    def delete_resource(self, resource_id):
        manifest = self._load()
        if resource_id not in manifest['resources'].keys():
            return False
        del manifest['resources'][resource_id]
        self._write(manifest)
        return True

    def add_job(self, job_id):
        manifest = self._load()

        manifest['jobs'][job_id] = {'status': 'enqueued',
                                    'timestamp': time.time()}
        self._write(manifest)

    def delete_job(self, job_id):
        manifest = self._load()
        if job_id not in manifest['jobs'].keys():
            return False
        del manifest['jobs'][job_id]
        self._write(manifest)
        return True

    def resource_exists(self, resource_id):
        manifest = self._load()
        return resource_id in manifest['resources'].keys()

    def resource_hash_exists(self, resource_hash):
        manifest = self._load()
        for resource_id, values in manifest['resources'].items():
            if values['hash'] == resource_hash:
                return resource_id
        return False

    def job_exists(self, job_id):
        manifest = self._load()
        return job_id in manifest['jobs'].keys()

    def get_job_status(self, job_id):
        manifest = self._load()
        if job_id not in manifest['jobs'].keys():
            return None
        return manifest['jobs'][job_id]['status']

    def get_job_exception_message(self, job_id):
        manifest = self._load()
        if job_id not in manifest['jobs'].keys():
            return None
        if 'exception_message' not in manifest['jobs'][job_id].keys():
            return None
        return manifest['jobs'][job_id]['exception_message']

    def update_job_status(self, job_id, status):
        manifest = self._load()
        if job_id not in manifest['jobs'].keys():
            return False
        manifest['jobs'][job_id]['status'] = status
        self._write(manifest)
        return True

    def add_job_exception(self, job_id, exception_message):
        manifest = self._load()
        if job_id not in manifest['jobs'].keys():
            return False
        manifest['jobs'][job_id]['status'] = 'exception'
        manifest['jobs'][job_id]['exception_message'] = exception_message
        self._write(manifest)
        return True

    def get_expired_jobs(self, lifespan):
        manifest = self._load()
        jobs_to_delete = []
        for job_id, values in manifest['jobs'].items():
            if time.time() - values['timestamp'] > lifespan:
                jobs_to_delete.append(job_id)

        return jobs_to_delete

    def get_expired_resources(self, lifespan):
        manifest = self._load()
        resources_to_delete = []
        for resource_id, values in manifest['resources'].items():
            if time.time() - values['timestamp'] > lifespan:
                resources_to_delete.append(resource_id)

        return resources_to_delete

    def delete_expired_jobs(self, lifespan):
        expired_jobs = self.get_expired_jobs(lifespan)
        manifest = self._load()
        for job_id in expired_jobs:
            del manifest['jobs'][job_id]
        self._write(manifest)
        return expired_jobs

    def delete_expired_resources(self, lifespan):
        expired_resources = self.get_expired_resources(lifespan)
        manifest = self._load()
        for resource_id in expired_resources:
            del manifest['resources'][resource_id]
        self._write(manifest)
        return expired_resources