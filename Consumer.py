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
            model.write_aggregated_results_to_json(job_filename + 'aggregate.json')
            if 'plot_choropleth_args' in orders:

                model.plot_choropleth(**orders['plot_choroplth_kwargs'])

    def execute_job(self, job):
        print('executing job:', job)
        if not os.path.exists('jobs/'):
            os.mkdir('jobs/')
        job_filename = 'jobs/' + job['job_id']
        os.mkdir(job_filename)


        if job['job_type'] == 'matrix':
            self.run_matrix_job(job['orders'], job_filename)
        elif job['job_type'] == 'model':
            self.run_model_job(job['orders'], job_filename)
        else:
            self.manifest.add_job_exception(job['job_id'], 'unknown_job_type')
            return
        self.manifest.update_job_status(job['job_id'], 'finished')
