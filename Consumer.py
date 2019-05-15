from multiprocessing import Process
import os

from spatial_access.p2p import TransitMatrix
from spatial_access.Models import TSFCA
from spatial_access.Models import Coverage
from spatial_access.Models import DestSum
from spatial_access.Models import AccessTime
from spatial_access.Models import AccessCount
from spatial_access.Models import AccessModel

from ResourceManagerExceptions import UnrecognizedJobTypeException
from ResourceManagerExceptions import MissingHintsException
from ResourceManagerExceptions import MissingColumnNamesException
from ResourceManagerExceptions import MissingResourceException

from Manifest import Manifest
from Job import Job


class Consumer(Process):

    def __init__(self, job_queue, manifest):
        Process.__init__(self)
        self.job_queue = job_queue
        self.manifest = manifest

    def run(self):
        while True:
            next_job = self.job_queue.get()
            self.execute_job(next_job)
            self.job_queue.task_done()

    @staticmethod
    def run_matrix_job(job):
        if 'primary_hints' not in job.orders['init_kwargs']:
            raise MissingHintsException('primary_hints')
        if job.secondary_resource is not None and 'secondary_hints' not in job.orders['init_kwargs']:
            raise MissingHintsException('secondary_hints')
        job.orders['init_kwargs']['primary_input'] = job.primary_resource
        job.orders['init_kwargs']['secondary_input'] = job.secondary_resource
        matrix = TransitMatrix(**job.orders['init_kwargs'])
        matrix.process()
        output_filename = job.job_folder + 'output.csv'
        matrix.write_csv(output_filename)

    @staticmethod
    def run_model_job(job):

        job.orders['init_kwargs']['sources_filename'] = job.primary_resource
        job.orders['init_kwargs']['destinations_filename'] = job.secondary_resource
        print('model job init_kwargs:', job.orders['init_kwargs'])
        if 'source_column_names' not in job.orders['init_kwargs']:
            raise MissingColumnNamesException('source_column_names')
        if 'dest_column_names' not in job.orders['init_kwargs']:
            raise MissingColumnNamesException('dest_column_names')
        if job.model_type == 'TSFCA':
            model = TSFCA(**job.orders['init_kwargs'])
        elif job.model_type == 'Coverage':
            model = Coverage(**job.orders['init_kwargs'])
        elif job.model_type == 'DestSum':
            model = DestSum(**job.orders['init_kwargs'])
        elif job.model_type == 'AccessTime':
            model = AccessTime(**job.orders['init_kwargs'])
        elif job.model_type == 'AccessCount':
            model = AccessCount(**job.orders['init_kwargs'])
        elif job.model_type == 'AccessModel':
            model = AccessModel(**job.orders['init_kwargs'])
        else:
            raise UnrecognizedJobTypeException(job.orders['model_type'])
        print('calculate_kwargs:', job.orders['calculate_kwargs'])
        model.calculate(**job.orders['calculate_kwargs'])
        if model.model_results is not None:
            model.write_results(job.job_folder + '/results.csv')
        print('wrote results')
        if 'aggregate_kwargs' in job.orders:
            print('aggregate_kwargs:', job.orders['aggregate_kwargs'])
            model.aggregate(**job.orders['aggregate_kwargs'])
            model.write_aggregated_results(job.job_folder + '/aggregate.json')

    def execute_job(self, job):
        print('executing job:', job.job_id)
        os.mkdir(job.job_folder)

        if job.job_type == 'matrix':
            try:
                self.run_matrix_job(job)
            except Exception as exception:
                print('exception:',str(exception))
                self.manifest.add_job_exception(job.job_id, str(exception))
                return
        elif job.job_type == 'model':
            try:
                self.run_model_job(job)
            except Exception as exception:
                self.manifest.add_job_exception(job.job_id, str(exception))
                return
        else:
            self.manifest.add_job_exception(job.job_id, 'unknown_job_type')
            return
        self.manifest.update_job_status(job.job_id, 'finished')
