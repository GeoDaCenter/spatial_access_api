from multiprocessing import Process
import os

from spatial_access.p2p import TransitMatrix
from spatial_access.CommunityAnalytics import DestFloatingCatchmentArea
from spatial_access.CommunityAnalytics import TwoStageFloatingCatchmentArea
from spatial_access.CommunityAnalytics import AccessTime
from spatial_access.CommunityAnalytics import AccessCount
from spatial_access.CommunityAnalytics import AccessModel

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
        if 'source_resource_id' not in job.orders['init_kwargs']:
            raise MissingResourceException('source_resource_id')
        elif 'dest_resource_id' not in job.orders['init_kwargs']:
            raise MissingResourceException('dest_resource_id')
        job.orders['init_kwargs']['source_resource'] = 'resources/' + job.orders['init_kwargs']['source_resource_id']
        job.orders['init_kwargs']['dest_resource'] = 'resources/' + job.orders['init_kwargs']['dest_resource_id']
        if 'source_column_names' not in job.orders['init_kwargs']:
            raise MissingColumnNamesException('source_column_names')
        if 'dest_column_names' not in job.orders['init_kwargs']:
            raise MissingColumnNamesException('dest_column_names')
        if job.orders['model_type'] == 'DestFloatingCatchmentArea':
            model = DestFloatingCatchmentArea(**job.orders['init_kwargs'])
        elif job.orders['model_type'] == 'TwoStageFloatingCatchmentArea':
            model = TwoStageFloatingCatchmentArea(**job.orders['init_kwargs'])
        elif job.orders['model_type'] == 'AccessTime':
            model = AccessTime(**job.orders['init_kwargs'])
        elif job.orders['model_type'] == 'AccessCount':
            model = AccessCount(**job.orders['init_kwargs'])
        elif job.orders['model_type'] == 'AccessModel':
            model = AccessModel(**job.orders['init_kwargs'])
        else:
            raise UnrecognizedJobTypeException(job.orders['model_type'])
        model.calculate(**job.orders['calculate_kwargs'])
        model_results = model.get_results()
        model_results.to_csv(job.job_folder + '/results.csv')
        if 'plot_cdf_kwargs' in job.orders:
            model.plot_cdf(**job.orders['plot_cdf_kwargs'])
        if 'aggregate_kwargs' in job.orders:
            aggregated_results = model.aggregate(**job.orders['aggregate_kwargs'])
            model.write_aggregated_results_to_json(job.job_folder + 'aggregate.json')
            if 'plot_choropleth_args' in job.orders:

                model.plot_choropleth(**job.orders['plot_choroplth_kwargs'])

    def execute_job(self, job):
        print('executing job:', job.job_id)
        os.mkdir(job.job_folder)

        if job.job_type == 'matrix':
            try:
                self.run_matrix_job(job)
            except:
                self.manifest.add_job_exception(job.job_id, "failed")
                return
        elif job.job_type == 'model':
            try:
                self.run_model_job(job)
            except:
                self.manifest.add_job_exception(job.job_id, "failed")
                return
        else:
            self.manifest.add_job_exception(job.job_id, 'unknown_job_type')
            return
        self.manifest.update_job_status(job.job_id, 'finished')
