# spatial_access API

ReST API for the spatial_access package.

https://github.com/GeoDaCenter/spatial_access

<table>  
<tr>
  <td>Version</td>
  <td>
   0.1.0 
   </td>
</tr>
</table>

# Install

`pip3 install -r requirements.txt`

# Run

`python3 server.py [--processes] [--resource_expiration] [--job_expiration]`


# Endpoints

## uploadResource

**URL** `/uploadResource`

**Method**: `PUT`

**Expects**: `{'file':file}`

### Responses

**Status Code**: `201`

**Reason**: Resource successfully uploaded

**Response**: `{'resource_id':resource_id}`

#### OR

**Status Code**: 400

**Reason**: `PUT` did not contain file

**Response**: `{}`

#### OR

**Status Code**: 403

**Reason**: Illegal file extension

**Response**: `{}`

## checkResourceById

**URL** `/checkResourceById/<resource_id>`

**Method**: `GET`

**Expects**: `{}`

### Responses

**Status Code**: `200`

**Reason**: Returned resource status

**Response**: `{'resource_id':resource_id, 'exists':'yes'|'no'}`

## checkResourceByHash

**URL** `/checkResourceById/<resource_hash>`

**Method**: `GET`

**Expects**: `{}`

### Responses

**Status Code**: `200`

**Reason**: Returned resource status

**Response**: `{'resource_id':resource_id, 'exists':'yes'|'no'}`

## deleteResource

**URL** `/deleteResource/<resource_id>`

**Method**: `DELETE`

**Expects**: `{}`

### Responses

**Status Code**: `200`

**Reason**: Resource successfully deleted

**Response**: `{}`

#### OR

**Status Code**: `403`

**Reason**: Resource does not exist

**Response**: `{}`

## submitJob

**URL** `/submitJob/<resource_id>`

**Method**: `POST`

**Expects**: `{'job':{...}}`

### Responses

**Status Code**: `200`

**Reason**: Job added to queue

**Response**: `{'job_id':job_id}`

#### OR

**Status Code**: `400`

**Reason**: Malformed `POST`

**Response**: `{}`

#### OR

**Status Code**: `500`

**Reason**: Server encountered an exception while processing job

**Response**: `{'exception':exception_message}`

## getResultsForJob

**URL** `/getResultsForJob/<job_id>`

**Method**: `GET`

**Expects**: `{}`

### Responses

**Status Code**: `200`

**Reason**: Returned job results

**Response**: `{'job_id':job_id, '''results':{...}}`

#### OR

**Status Code**: `404`

**Reason**: Job not found

**Response**: `{'job_id':job_id}`

#### OR

**Status Code**: `500`

**Reason**: Server encountered an exception while processing job

**Response**: `{'job_id':job_id, 'exception':exception_message}`

## checkJobStatus

**URL** `/checkJobStatus/<job_id>`

**Method**: `GET`

**Expects**: `{}`

### Responses

**Status Code**: `200`

**Reason**: Job status returned

**Response**: `{'job_id':job_id, 'status':'enqueued' | 'not_found' | 'completed'}`

## cancelJob

**URL** `/cancelJob/<job_id>`

**Method**: `POST`

**Expects**: `{}`

### Responses

**Status Code**: `200`

**Reason**: Job cancelled

**Response**: `{'job_id':job_id}`

#### OR

**Status Code**: `403`

**Reason**: Illegal request

**Response**: `{'job_id':job_id}`

## deleteJobResults

**URL** `/deleteJobResults/<job_id>`

**Method**: `DELETE`

**Expects**: `{}`

### Responses

**Status Code**: `200`

**Reason**: Job results deleted

**Response**: `{'job_id':job_id}`

#### OR

**Status Code**: `403`

**Reason**: Illegal request

**Response**: `{'job_id':job_id}`