import uuid
import os
import json


class Job:
    def __init__(self, request):
        self.error_status = None

        # try parse request
        try:
            job_request = request.to_dict()
        except:
            self.error_status = "unparsable request"
            return

        # expect valid job type
        if 'job_type' not in job_request.keys():
            self.error_status = "missing job_type"
            return
        self.job_type = job_request['job_type']
        if job_request['job_type'] not in ['matrix', 'model']:
            self.error_status = "invalid job type: {}".format(job_request['job_type'])
            return
        # expect orders
        if 'orders' not in job_request.keys():
            self.error_status = 'missing orders'
            return
        try:
            self.orders = json.loads(job_request['orders'])
        except:
            self.error_status = "orders not parsable"
            return
        print('init+kwargs:', self.orders['init_kwargs'])
        # expect primary_resource to be present
        if 'primary_resource' not in job_request.keys():
            self.error_status = "primary_resource not specified"
            return
        self.primary_resource = 'resources/' + job_request['primary_resource']
        if not os.path.exists(self.primary_resource):
            self.error_status = "missing primary_resource"
            return

        # if secondary_resource specified, check that it is present
        if 'secondary_resource' in job_request.keys():
            self.secondary_resource = 'resources/' + job_request['secondary_resource']
            if not os.path.exists(self.secondary_resource):
                self.error_status = "missing secondary_resource"
                return
        else:
            self.secondary_resource = None

        # generate a new uuid
        self.job_id = self.get_new_job_id()
        self.job_folder = 'jobs/' + self.job_id + '/'

    @staticmethod
    def get_new_job_id():
        return uuid.uuid4().hex