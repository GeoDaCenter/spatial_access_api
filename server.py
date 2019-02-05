from flask import Flask, request, Response, jsonify
from ResourceManager import ResourceManager, ResourceDoesNotExistException
import json

resource_manager = ResourceManager()


application = Flask(__name__)
application.config['MAX_CONTENT_LENGTH'] = 536870912 # 1/2 GB
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
    return jsonify(file_id=resource_id), 201


@application.route('/checkResourceById/<resource_id>', methods=['GET'])
def check_resource_by_id(resource_id):
    if resource_manager.resource_id_exists(resource_id=resource_id):
        return jsonify(exists='yes'), 200
    return jsonify(exists='no'), 200


@application.route('/checkResourceByHash/<resource_hash>', methods=['GET'])
def check_resource_by_hash(resource_hash):
    if resource_manager.resource_hash_exists(resource_hash=resource_hash):
        return jsonify(exists='yes'), 200
    return jsonify(exists='no'), 200


@application.route('/deleteResource/<resource_id>', methods=['DELETE'])
def delete_resource(resource_id):
    if '/' in resource_id:
        return Response(status=403)
    try:
        resource_manager.delete_resource(resource_id)
        return Response(status=200)
    except ResourceDoesNotExistException:
        return Response(status=403)


@application.route('/submitJob', methods=['POST'])
def submit_job():
    if 'job' in request.values:
        job = json.loads(request.values.to_dict()['job'])
        job_id = resource_manager.get_new_job_id()
        job['job_id'] = job_id
        resource_manager.add_job_to_queue(job)
        return jsonify(job_id=job_id), 200
    return Response(status=400)


@application.route('/checkJobStatus/<job_id>', methods=['GET'])
def check_job_status(job_id):
    status = resource_manager.get_job_status(job_id)
    return jsonify(status=status), 200


@application.route('/cancelJob/<job_id>', methods=['POST'])
def cancel_job(job_id):
    if resource_manager.cancel_job(job_id):
        return Response(status=200)
    return Response(status=403)


@application.route('/deleteJobResults/<job_id>', methods=['DELETE'])
def delete_job_results(job_id):
    if '/' in job_id:
        return Response(status=403)
    if resource_manager.delete_job_results(job_id):
        return Response(status=200)
    return Response(status=403)


@application.route('/getResultsForJob/<job_id>', methods=['GET'])
def get_results_for_job(job_id):
    job_results = resource_manager.get_job_results(job_id)
    if not job_results:
        return Response(status=404)
    elif 'exception' in job_results.keys():
        return jsonify(exception=job_results['exception']), 500
    else:
        return Response(status=200)

if __name__ == "__main__":
    application.run()