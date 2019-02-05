import uuid
import os
import json
import hashlib
import time
import multiprocessing

from spatial_access.p2p import TransitMatrix
from spatial_access.CommunityAnalytics import DestFloatingCatchmentArea
from spatial_access.CommunityAnalytics import TwoStageFloatingCatchmentArea
from spatial_access.CommunityAnalytics import AccessTime
from spatial_access.CommunityAnalytics import AccessCount
from spatial_access.CommunityAnalytics import AccessModel
from ResourceManagerExceptions import ResourceDoesNotExistException
from ResourceManagerExceptions import UnrecognizedJobTypeException
from ResourceManagerExceptions import MissingHintsException
from ResourceManagerExceptions import MissingColumnNamesException
from ResourceManagerExceptions import MissingResourceException


class Consumer(multiprocessing.Process):

    def __init__(self, job_queue):
        multiprocessing.Process.__init__(self)
        self.job_queue = job_queue

    def run(self):
        while True:
            next_job = self.job_queue.get()
            self.execute_job(next_job)
            self.job_queue.task_done()

    @staticmethod
    def run_matrix_job(orders, job_filename):
        if 'primary_hints' not in orders['init_kwargs']:
            raise MissingHintsException('primary_hints')
        if 'secondary_input' in orders['init_kwargs'] and 'secondary_hints' not in orders['init_kwargs']:
            raise MissingHintsException('secondary_hints')
        if 'primary_resource_id' not in orders['init_kwargs']:
            raise MissingResourceException('primary_resource_id')
        else:
            orders['init_kwargs']['primary_input'] = 'resources/' + orders['init_kwargs']['primary_resource_id']
        if 'secondary_resource_id' in orders['init_kwargs']:
            orders['init_kwargs']['secondary_input'] = 'resources/' + orders['init_kwargs']['secondary_resource_id']
        else:
            orders['init_kwargs']['secondary_input'] = None
        matrix = TransitMatrix(**orders['init_kwargs'])
        matrix.process()
        matrix.write_csv(job_filename + '/output.csv')

    @staticmethod
    def run_model_job(orders, job_filename):
        if 'source_resource_id' not in orders['init_kwargs']:
            raise MissingResourceException('source_resource_id')
        elif 'dest_resource_id' not in orders['init_kwargs']:
            raise MissingResourceException('dest_resource_id')
        orders['init_kwargs']['source_resource'] = 'resources/' + orders['init_kwargs']['source_resource_id']
        orders['init_kwargs']['dest_resource'] = 'resources/' + orders['init_kwargs']['dest_resource_id']
        if 'source_column_names' not in orders['init_kwargs']:
            raise MissingColumnNamesException('source_column_names')
        if 'dest_column_names' not in orders['init_kwargs']:
            raise MissingColumnNamesException('dest_column_names')
        if orders['model_type'] == 'DestFloatingCatchmentArea':
            model = DestFloatingCatchmentArea(**orders['init_kwargs'])
        elif orders['model_type'] == 'TwoStageFloatingCatchmentArea':
            model = TwoStageFloatingCatchmentArea(**orders['init_kwargs'])
        elif orders['model_type'] == 'AccessTime':
            model = AccessTime(**orders['init_kwargs'])
        elif orders['model_type'] == 'AccessCount':
            model = AccessCount(**orders['init_kwargs'])
        elif orders['model_type'] == 'AccessModel':
            model = AccessModel(**orders['init_kwargs'])
        else:
            raise UnrecognizedJobTypeException(orders['model_type'])
        model.calculate(**orders['calculate_kwargs'])
        model_results = model.get_results()
        model_results.to_csv(job_filename + '/results.csv')
        if 'plot_cdf_kwargs' in orders:
            model.plot_cdf(**orders['plot_cdf_kwargs'])
        if 'aggregate_kwargs' in orders:
            aggregated_results = model.aggregate(**orders['aggregate_kwargs'])
            aggregated_results.to_csv('/aggregated_results.csv')
            if 'plot_choropleth_args' in orders:
                model.plot_choropleth(**orders['plot_choroplth_kwargs'])

    def execute_job(self, job):
        print('executing job:', job)
        if not os.path.exists('jobs/'):
            os.mkdir('jobs/')
        job_filename = 'jobs/' + job['job_id']
        os.mkdir(job_filename)
        job_meta = {}
        try:
            if job['job_type'] == 'matrix':
                self.run_matrix_job(job['orders'], job_filename)
            elif job['job_type'] == 'model':
                self.run_model_job(job['orders'], job_filename)
            else:
                job_meta['exception'] = 'unrecognized_job_type'
        except Exception as exception:
            job_meta['exception'] = str(exception)

        job_meta['timestamp'] = time.time()
        with open(job_filename, 'w') as file:
            json.dump(job_meta, file)


class ResourceManager:
    def __init__(self, num_processes=2, resource_lifespan=86400, job_lifespan=86400):
        self.allowed_extensions = {'csv', 'png'}
        self.resource_lifespan = resource_lifespan
        self.job_lifespan = job_lifespan
        self.num_processes = num_processes
        self.job_queue = None
        self.consumers = None

    def start(self):
        self.job_queue = multiprocessing.JoinableQueue()
        self.consumers = [Consumer(self.job_queue) for _ in range(self.num_processes)]
        for consumer in self.consumers:
            consumer.start()

    def shutdown(self):
        for consumer in self.consumers:
            consumer.terminate()

    def get_job_status(self, job_id):
        pass

    def get_job_results(self, job_id):
        path = 'jobs/' + job_id
        if os.path.exists(path):
            meta_file = path + '/meta.json'
            return self.load_job_meta(meta_file)
        return None

    def add_job_to_queue(self, job):
        self.job_queue.put(job)

    def delete_job_results(self, job_id):
        path = 'jobs/' + job_id
        if os.path.exists(path):
            try:
                os.removedirs(path)
                return True
            except BaseException:
                return False
        return False

    @staticmethod
    def load_job_meta(meta_file):
        if os.path.exists(meta_file):
            with open(meta_file, 'r') as file:
                return json.load(file)

    def delete_expired_jobs(self):
        if os.path.exists('jobs/'):
            for job_file in os.listdir('jobs/'):
                job_meta = self.load_job_meta('jobs/' + job_file + '/meta.json')
                if time.time() - job_meta['timestamp'] > self.job_lifespan:
                    os.rmdir(job_file)

    @staticmethod
    def get_manifest():
        if os.path.exists('manifest.json'):
            with open('manifest.json', 'r') as file:
                return json.load(file)
        else:
            return {}

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
        manifest[resource_id] = {'hash': resource_hash, 'timestamp': time.time()}
        self.write_manifest(manifest)

    def delete_resource(self, resource_id):
        manifest = self.get_manifest()
        if resource_id not in manifest.keys():
            raise ResourceDoesNotExistException(resource_id)
        del manifest[resource_id]
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
        return resource_id in manifest.keys()

    def resource_hash_exists(self, resource_hash):
        manifest = self.get_manifest()
        for resource_id, resource in manifest.items():
            if resource['hash'] == resource_hash:
                return resource_id
        return None

    def delete_expired_resources(self):
        resources_to_delete = []
        manifest = self.get_manifest()
        for resource_id, resource_details in manifest.items():
            if time.time() - resource_details['timestamp'] > self.resource_lifespan:
                resources_to_delete.append(resource_id)

        for resource_id in resources_to_delete:
            self.delete_resource(resource_id)
