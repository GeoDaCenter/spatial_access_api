from flask import Flask, request, Response, jsonify, send_file
from ResourceManager import ResourceManager
from Job import Job
import signal
import sys
import argparse

parser = argparse.ArgumentParser(description='Start the spatial_access ReST API')
parser.add_argument('--num_workers', metavar='-w', type=int, default=2,
                    help='How many concurrent workers should be allowed to'
                         'run spatial_access jobs')
parser.add_argument('--resource_expiration', metavar='-r', type=int,
                    default=86400, help='Expiration (in seconds) of resources.')
parser.add_argument('--job_expiration', metavar='-j', type=int,
                    default=86400, help='Expiration (in seconds) of job results.')
parser.add_argument('--max_file_size', metavar='-m', type=int,
                    default=536870912, help='Max file size (in bytes) to allow users to upload.')
parser.add_argument('--port', metavar='-p', type=int,
                    default=5000, help='Port number to listen on.')
parser.add_argument('--deploy', action='store_true', help='Deploy to accept incoming connections.')
args = parser.parse_args()

resource_manager = ResourceManager(num_processes=args.num_workers,
                                   resource_lifespan=args.resource_expiration,
                                   job_lifespan=args.job_expiration)


def sigint_handler(sig, frame):
    print('Shutting down...')
    resource_manager.shutdown()
    sys.exit(0)


# register sigint handler
signal.signal(signal.SIGINT, sigint_handler)

resource_manager.start()

application = Flask(__name__)
application.config['MAX_CONTENT_LENGTH'] = args.max_file_size
application.config['PROPAGATE_EXCEPTIONS'] = True


@application.route('/uploadResource', methods=['POST'])
def upload_resource():

    if 'file' not in request.files:
        return Response(status=400)
    resource_id = resource_manager.get_new_resource_id()
    file = request.files['file']
    if not resource_manager.extension_is_allowed(filename=file.filename):
        return Response(status=403)
    file.save("resources/" + resource_id)
    resource_manager.add_resource(resource_id)
    return jsonify(resource_id=resource_id), 201


@application.route('/checkResourceById/<resource_id>', methods=['GET'])
def check_resource_by_id(resource_id):
    if resource_manager.resource_id_exists(resource_id=resource_id):
        return jsonify(resource_id=resource_id,
                       exists='yes'), 200
    return jsonify(resource_id=resource_id,
                   exists='no'), 200


@application.route('/checkResourceByHash/<resource_hash>', methods=['GET'])
def check_resource_by_hash(resource_hash):
    resource_id = resource_manager.resource_hash_exists(resource_hash=resource_hash)
    if resource_id:
        return jsonify(resource_id=resource_id), 200
    return Response(404)


@application.route('/deleteResource/<resource_id>', methods=['DELETE'])
def delete_resource(resource_id):
    if not resource_manager.job_id_is_safe(resource_id):
        return Response(status=403)
    if resource_manager.delete_resource(resource_id):
        return Response(status=200)
    else:
        return Response(status=403)


@application.route('/submitJob', methods=['POST'])
def submit_job():
    job = Job(request.values)
    if job.error_status is not None:
        return jsonify(error=job.error_status), 400
    resource_manager.add_job_to_queue(job)
    return jsonify(job_id=job.job_id), 200


@application.route('/checkJobStatus/<job_id>', methods=['GET'])
def check_job_status(job_id):
    if not resource_manager.manifest.job_exists(job_id):
        return jsonify(job_id=job_id), 404
    job_status = resource_manager.get_job_status(job_id)
    if job_status == 'exception' or job_status == 'failed':
        exception_message  = resource_manager.manifest.get_job_exception_message(job_id)
        return jsonify(job_id=job_id,
                       job_status=job_status,
                       exception_message=exception_message), 500
    return jsonify(job_id=job_id, job_status=job_status), 200


@application.route('/deleteJobResults/<job_id>', methods=['DELETE'])
def delete_job_results(job_id):
    if not resource_manager.job_id_is_safe(job_id):
        return Response(status=403)
    if resource_manager.delete_job_results(job_id):
        return Response(status=200)
    else:
        return Response(status=403)


@application.route('/getResultsForJob/<job_id>', methods=['GET'])
def get_results_for_job(job_id):
    if not resource_manager.job_id_is_safe(job_id):
        return jsonify(job_id=job_id), 403
    job_status = resource_manager.get_job_status(job_id)
    if job_status == 'exception':
        exception_message  = resource_manager.manifest.get_job_exception_message(job_id)
        return jsonify(job_id=job_id, exception_message=exception_message), 500
    zip_filename = resource_manager.get_zip_filename(job_id)
    if zip_filename is not None:
        return send_file(zip_filename)
    return jsonify(job_id=job_id), 404


@application.route('/getAggregatedResultsForJob/<job_id>', methods=['GET'])
def get_aggregated_results_for_job(job_id):
    if not resource_manager.job_id_is_safe(job_id):
        return jsonify(job_id=job_id), 403
    results = resource_manager.get_aggregated_data(job_id)
    if results is not None:
        return jsonify(results=results), 200
    return jsonify(job_id=job_id), 404


if __name__ == "__main__":
    if args.deploy:
        hostname='0.0.0.0'
    else:
        hostname = '127.0.0.1'
    application.run(host=hostname, port=args.port)
