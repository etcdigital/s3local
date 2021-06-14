'use strict';

const Busboy = require('busboy');
const crypto = require('crypto');
const { once } = require('events');
const { URL } = require('url');

const { DUMMY_ACCOUNT } = require('../models/account');
const S3Error = require('../models/error');
const S3Event = require('../models/event');
const S3Object = require('../models/object');
const { TaggingConfiguration } = require('../models/config');
const { capitalizeHeader, utf8BodyParser, xmlBodyParser } = require('../utils');
const { aws4SignatureBodyParser } = require('../signature/v4');

function triggerS3Event(ctx, eventData) {
  ctx.app.emit(
    'event',
    new S3Event(eventData, {
      reqHeaders: ctx.headers,
      sourceIp: ctx.ip,
    }),
  );
}

/*
 * Operations on Objects
 * The following methods correspond to operations you can perform on Amazon S3 objects.
 * https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectOps.html
 */

exports.METHODS = [
  'acl',
  'legal-hold',
  'retention',
  'select',
  'tagging',
  'torrent',
  'uploadId',
  'uploads',
];

/**
 * Delete Multiple Objects
 * The Multi-Object Delete request contains a list of up to 1000 Keys that you want to delete. In
 * the XML, you provide the object Key names, and optionally, version IDs if you want to delete a
 * specific version of the object from a versioning-enabled bucket.
 * {@link https://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html}
 */
exports.deleteMultipleObjects = async function deleteMultipleObjects(ctx) {
  await xmlBodyParser(ctx);
  if (!ctx.request.body.Delete || !ctx.request.body.Delete.Object) {
    throw new S3Error(
      'MalformedXML',
      'The XML you provided was not well-formed or did not validate against ' +
        'our published schema.',
    );
  }
  const Keys = [].concat(ctx.request.body.Delete.Object).map((o) => o.Key);
  await Promise.all(
    Keys.map(async (Key) => {
      const objectExists = await ctx.store.existsObject(ctx.params.bucket, Key);
      if (!objectExists) return;

      await ctx.store.deleteObject(ctx.params.bucket, Key);
      ctx.logger.info(
        'Deleted object "%s" in bucket "%s"',
        Key,
        ctx.params.bucket,
      );
    }),
  );
  ctx.body = {
    DeleteResult: {
      '@': { xmlns: 'http://s3.amazonaws.com/doc/2006-03-01/' },
      Deleted: Keys.map((k) => ({ Key: k })),
    },
  };
};

/**
 * DELETE Object
 * The DELETE operation removes the null version (if there is one) of an object and inserts a
 * delete marker, which becomes the current version of the object. If there isn't a null version,
 * Amazon S3 does not remove any objects.
 * {@link https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html}
 */
exports.deleteObject = async function deleteObject(ctx) {
  try {
    if (await ctx.store.existsObject(ctx.params.bucket, ctx.params.Key)) {
      await ctx.store.deleteObject(ctx.params.bucket, ctx.params.Key);
      triggerS3Event(ctx, {
        bucket: ctx.params.bucket,
        eventType: 'Delete',
        S3Item: { Key: ctx.params.Key },
      });
    }
    ctx.status = 204;
  } catch (err) {
    ctx.logger.error(
      'Error deleting object "%s" from bucket "%s"',
      ctx.params.Key,
      ctx.params.bucket,
      err,
    );
    throw err;
  }
};

/**
 * GET Object
 * This implementation of the GET operation retrieves objects from Amazon S3. To use GET, you must
 * have READ access to the object. If you grant READ access to the anonymous user, you can return
 * the object without using an authorization header.
 * {@link https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html}
 */
exports.getObject = async function getObject(ctx) {
  const options = {};
  if (/^bytes=/.test(ctx.headers.range)) {
    const [start, end] = ctx.headers.range.replace('bytes=', '').split('-');
    options.start = Number(start);
    if (end) options.end = Number(end);
  }

  const Key = ctx.params.Key;
  const object = await ctx.store.getObject(ctx.params.bucket, Key, options);
  if (!object) {
    throw new S3Error('NoSuchKey', 'The specified Key does not exist.', {
      Key,
    });
  }

  // Range request was out of range
  if (object.range && !object.content) {
    throw new S3Error(
      'InvalidRange',
      'The requested range is not satisfiable',
      {
        RangeRequested: ctx.get('range'),
        ActualObjectSize: object.size,
      },
    );
  }

  const noneMatch = ctx.headers['if-none-match'];
  if (noneMatch && (noneMatch === object.metadata.etag || noneMatch === '*')) {
    ctx.status = 304;
    return;
  }
  const modifiedSince = ctx.headers['if-modified-since'];
  if (new Date(modifiedSince) >= object.lastModifiedDate) {
    ctx.status = 304;
    return;
  }

  ctx.set('Accept-Ranges', 'bytes');

  for (const header of Object.keys(object.metadata)) {
    ctx.set(capitalizeHeader(header), object.metadata[header]);
  }
  if (object.range) {
    ctx.set(
      'Content-Range',
      `bytes ${object.range.start}-${object.range.end}/${object.size}`,
    );
    ctx.length = object.range.end + 1 - object.range.start;
    ctx.status = 206;
  } else {
    ctx.length = object.size;
    ctx.status = 200;
  }

  if (ctx.method === 'HEAD') {
    object.content.destroy();
  } else {
    ctx.body = object.content;
  }
};

/**
 * GET Object ACL
 * This implementation of the GET operation uses the acl subresource to return the access control
 * list (ACL) of an object. To use this operation, you must have READ_ACP access to the object.
 * {@link https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGETacl.html}
 */
exports.getObjectAcl = async function getObjectAcl(ctx) {
  // NOTE: This is a stub implemenatation
  ctx.body = {
    AccessControlPolicy: {
      '@': { xmlns: 'http://doc.s3.amazonaws.com/2006-03-01/' },
      Owner: {
        ID: DUMMY_ACCOUNT.id,
        DisplayName: DUMMY_ACCOUNT.displayName,
      },
      AccessControlList: {
        Grant: {
          Grantee: {
            '@': {
              'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
              'xsi:type': 'CanonicalUser',
            },
            ID: 'abc',
            DisplayName: 'You',
          },
        },
        Permission: 'FULL_CONTROL',
      },
    },
  };
};

/**
 * GET Object tagging
 * This implementation of the GET operation returns the tags associated with an object. You
 * send the GET request against the tagging subresource associated with the object.
 * {@link https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGETtagging.html}
 *
 * @param {Koa.Context} ctx
 * @returns {Promise<void>}
 */
exports.getObjectTagging = async function getObjectTagging(ctx) {
  const Key = ctx.params.Key;
  const exists = await ctx.store.existsObject(ctx.params.bucket, Key);
  if (!exists) {
    throw new S3Error('NoSuchKey', 'The specified Key does not exist.', {
      Key,
    });
  }

  const config = await ctx.store.getSubresource(
    ctx.params.bucket,
    Key,
    'tagging',
  );

  ctx.type = 'application/xml';
  ctx.body = (config || TaggingConfiguration.EMPTY).toXML();
};

/**
 * POST Object
 * The POST operation adds an object to a specified bucket using HTML forms.
 * {@link https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html}
 */
exports.postObject = async function postObject(ctx) {
  if (!ctx.is('multipart/form-data')) {
    throw new S3Error(
      'PreconditionFailed',
      'At least one of the pre-conditions you specified did not hold',
      {
        Condition:
          'Bucket POST must be of the enclosure-type multipart/form-data',
      },
    );
  }
  try {
    let successRedirect;
    let successStatus = 204;
    let fileCount = 0;

    const busboy = new Busboy({ headers: ctx.headers });
    const finishedPromise = once(busboy, 'finish');
    const object = await Promise.race([
      finishedPromise.then(() => null),
      new Promise((resolve, reject) => {
        let Key;
        const metadata = {};

        const errorHandler = (err) => {
          busboy.off('field', fieldHandler);
          busboy.off('file', fileHandler);
          reject(err);
        };
        const fieldHandler = (fieldname, value) => {
          if (fieldname === 'file') {
            // busboy will identify parts as fields if they both lack a filename
            // and have a content-type other than application/octet-stream
            const file = Buffer.from(value);
            fileCount++;
            resolve(new S3Object(ctx.params.bucket, Key, file, metadata));
          }
          if (fileCount) {
            // ignore everything submitted after the first 'file' field
            return;
          }
          switch (fieldname) {
            case 'Key':
              Key = value;
              break;
            case 'success_action_redirect':
              successRedirect = value;
              break;
            case 'redirect':
              // redirect field name is deprecated and is always overridden by
              // success_action_redirect if present
              if (successRedirect === undefined) {
                successRedirect = value;
              }
              break;
            case 'success_action_status':
              if (value === '200' || value === '201') {
                successStatus = Number(value);
              }
              break;
            case 'AWSAccessKeyId':
            case 'acl':
            case 'policy':
            case 'tagging':
              // unimplemented
              break;
            default:
              metadata[fieldname.toLowerCase()] = value;
              break;
          }
        };
        const fileHandler = (fieldname, file) => {
          if (fieldname !== 'file' || fileCount++) {
            return file.resume();
          }
          resolve(new S3Object(ctx.params.bucket, Key, file, metadata));
        };

        busboy.once('error', errorHandler);
        busboy.on('field', fieldHandler);
        busboy.on('file', fileHandler);

        ctx.req.pipe(busboy);
      }),
    ]);

    if (!object) {
      throw new S3Error(
        'InvalidArgument',
        'POST requires exactly one file upload per request.',
        {
          ArgumentName: 'file',
          ArgumentValue: fileCount,
        },
      );
    }

    if (object.Key === undefined) {
      throw new S3Error(
        'InvalidArgument',
        "Bucket POST must contain a field named 'Key'.  If it is specified, " +
          'please check the order of the fields',
        {
          ArgumentName: 'Key',
          ArgumentValue: '',
        },
      );
    }

    if (!object.Key.length) {
      throw new S3Error(
        'InvalidArgument',
        'User Key must have a length greater than 0.',
        {
          ArgumentName: 'Key',
          ArgumentValue: '',
        },
      );
    }

    const { md5, size } = await ctx.store.putObject(object);
    ctx.logger.info(
      'Stored object "%s" in bucket "%s" successfully',
      object.Key,
      object.bucket,
    );
    triggerS3Event(ctx, {
      bucket: ctx.params.bucket,
      eventType: 'Post',
      S3Item: new S3Object(object.bucket, object.Key, null, {
        'content-length': size,
        etag: JSON.stringify(md5),
      }),
    });

    if (!busboy._finished) {
      await finishedPromise;
    }

    if (fileCount > 1) {
      ctx.logger.warn(
        'A POST request for object %s in bucket %s contained %d files in the ' +
          'multipart body. This request would be rejected by real S3 servers.',
        object.Key,
        object.bucket,
        fileCount,
      );
    }

    ctx.etag = md5;

    if (successRedirect) {
      ctx.status = 303;
      const location = new URL(successRedirect);
      location.searchParams.append('bucket', object.bucket);
      location.searchParams.append('Key', object.Key);
      location.searchParams.append('etag', ctx.response.get('ETag'));
      ctx.set('Location', location.href);
    } else {
      ctx.status = successStatus;
      const location = new URL(ctx.originalUrl, ctx.origin);
      if (!location.pathname.endsWith('/')) {
        location.pathname += '/';
      }
      location.pathname += object.Key;
      ctx.set('Location', location.href);
    }

    if (ctx.status === 201) {
      ctx.body = {
        PostResponse: {
          Location: ctx.response.get('Location'),
          Bucket: object.bucket,
          Key: object.Key,
          Etag: JSON.stringify(md5),
        },
      };
    } else {
      ctx.body = '';
      ctx.type = '';
    }
  } catch (err) {
    ctx.logger.error(
      'Error uploading object to bucket "%s"',
      ctx.params.bucket,
      err,
    );
    throw err;
  }
};

/**
 * PUT Object
 * This implementation of the PUT operation adds an object to a bucket.
 * {@link https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html}
 */
exports.putObject = async function putObject(ctx) {
  aws4SignatureBodyParser(ctx);
  const object = new S3Object(
    ctx.params.bucket,
    ctx.params.Key,
    ctx.request.body,
    ctx.headers,
  );
  try {
    const { md5, size } = await ctx.store.putObject(object);
    ctx.logger.info(
      'Stored object "%s" in bucket "%s" successfully',
      object.Key,
      object.bucket,
    );
    triggerS3Event(ctx, {
      bucket: ctx.params.bucket,
      eventType: 'Put',
      S3Item: new S3Object(object.bucket, object.Key, null, {
        'content-length': size,
        etag: JSON.stringify(md5),
      }),
    });
    ctx.etag = md5;
    ctx.body = '';
  } catch (err) {
    ctx.logger.error(
      'Error uploading object "%s" to bucket "%s"',
      object.Key,
      object.bucket,
      err,
    );
    throw err;
  }
};

/**
 * PUT Object - Copy
 * This implementation of the PUT operation creates a copy of an object that is already stored in
 * Amazon S3. A PUT copy operation is the same as performing a GET and then a PUT. Adding the
 * request header, x-amz-copy-source, makes the PUT operation copy the source object into the
 * destination bucket.
 * {@link https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html}
 */
exports.putObjectCopy = async function putObjectCopy(ctx) {
  let copySource = decodeURI(ctx.headers['x-amz-copy-source']);
  copySource = copySource.startsWith('/') ? copySource.slice(1) : copySource;
  let [srcBucket, ...srcKey] = copySource.split('/');
  srcKey = srcKey.join('/');

  const destBucket = ctx.params.bucket;
  const destKey = ctx.params.Key;

  try {
    const bucket = await ctx.store.getBucket(srcBucket);
    if (!bucket) {
      ctx.logger.error('No bucket found for "%s"', srcBucket);
      throw new S3Error('NoSuchBucket', 'The specified bucket does not exist', {
        BucketName: srcBucket,
      });
    }

    if (!(await ctx.store.existsObject(srcBucket, srcKey))) {
      ctx.logger.error(
        'Object "%s" in bucket "%s" does not exist',
        srcKey,
        srcBucket,
      );
      throw new S3Error('NoSuchKey', 'The specified Key does not exist.', {
        Key: srcKey,
      });
    }
    const replaceMetadata =
      ctx.headers['x-amz-metadata-directive'] === 'REPLACE';
    if (srcBucket === destBucket && srcKey === destKey && !replaceMetadata) {
      throw new S3Error(
        'InvalidRequest',
        'This copy request is illegal because it is trying to copy an object ' +
          "to itself without changing the object's metadata, storage class, " +
          'website redirect location or encryption attributes.',
      );
    }

    const metadata = await ctx.store.copyObject(
      srcBucket,
      srcKey,
      destBucket,
      destKey,
      replaceMetadata ? ctx.headers : null,
    );

    ctx.logger.info(
      'Copied object "%s" from bucket "%s" into bucket "%s" with Key of "%s"',
      srcKey,
      srcBucket,
      destBucket,
      destKey,
    );
    triggerS3Event(ctx, {
      bucket: ctx.params.bucket,
      eventType: 'Copy',
      S3Item: new S3Object(destBucket, destKey, null, metadata),
    });
    ctx.body = {
      CopyObjectResult: {
        LastModified: new Date(metadata['last-modified']).toISOString(),
        ETag: metadata.etag,
      },
    };
  } catch (err) {
    ctx.logger.error(
      'Error copying object "%s" from bucket "%s" into bucket "%s" with Key of "%s"',
      srcKey,
      srcBucket,
      destBucket,
      destKey,
    );
    throw err;
  }
};

/**
 * PUT Object tagging
 * This implementation of the PUT operation uses the tagging subresource to add a set of tags
 * to an existing object.
 * {@link https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUTtagging.html}
 *
 * @param {Koa.Context} ctx
 * @returns {Promise<void>}
 */
exports.putObjectTagging = async function putObject(ctx) {
  const Key = ctx.params.Key;
  const bucket = ctx.params.bucket;
  const exists = await ctx.store.existsObject(bucket, Key);
  if (!exists) {
    throw new S3Error('NoSuchKey', 'The specified Key does not exist.', {
      Key,
    });
  }

  try {
    await utf8BodyParser(ctx);
    const config = TaggingConfiguration.validate(ctx.request.body);
    await ctx.store.putSubresource(bucket, Key, config);
    ctx.body = '';
  } catch (err) {
    ctx.logger.error(
      'Error tagging object "%s" in bucket "%s"',
      Key,
      bucket,
      err,
    );
    throw err;
  }
};

/**
 * Complete Multipart Upload
 * This operation completes a multipart upload by assembling previously uploaded parts.
 * {@link https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html}
 */
exports.completeMultipartUpload = async function completeMultipartUpload(ctx) {
  await xmlBodyParser(ctx);
  if (
    !ctx.request.body.CompleteMultipartUpload ||
    !ctx.request.body.CompleteMultipartUpload.Part
  ) {
    throw new S3Error(
      'MalformedXML',
      'The XML you provided was not well-formed or did not validate against ' +
        'our published schema.',
    );
  }
  const parts = []
    .concat(ctx.request.body.CompleteMultipartUpload.Part)
    .map((part) => ({
      number: Number(part.PartNumber),
      // S3 removes all double quotes when comparing etags
      etag: part.ETag.replace(/"/g, ''),
    }));
  try {
    const { md5, size } = await ctx.store.putObjectMultipart(
      ctx.params.bucket,
      ctx.query.uploadId,
      parts,
    );
    ctx.logger.info(
      'Stored object "%s" in bucket "%s" successfully',
      ctx.params.Key,
      ctx.params.bucket,
    );
    triggerS3Event(ctx, {
      bucket: ctx.params.bucket,
      eventType: 'Post',
      S3Item: new S3Object(ctx.params.bucket, ctx.params.Key, null, {
        etag: JSON.stringify(md5),
        'content-length': size,
      }),
    });
    ctx.body = {
      CompleteMultipartUploadResult: {
        Location: ctx.href.split('?')[0],
        Bucket: ctx.params.bucket,
        Key: ctx.params.Key,
        ETag: JSON.stringify(md5),
      },
    };
  } catch (err) {
    ctx.logger.error(
      'Error uploading object "%s" to bucket "%s"',
      ctx.params.Key,
      ctx.params.bucket,
      err,
    );
    throw err;
  }
};

/**
 * Initiate Multipart Upload
 * This operation initiates a multipart upload and returns an upload ID. This upload ID is used to
 * associate all of the parts in the specific multipart upload. You specify this upload ID in each
 * of your subsequent upload part requests. You also include this upload ID in the final request to
 * either complete or abort the multipart upload request.
 * {@link https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadInitiate.html}
 */
exports.initiateMultipartUpload = async function initiateMultipartUpload(ctx) {
  const uploadId = crypto.randomBytes(16).toString('hex');
  await ctx.store.initiateUpload(
    ctx.params.bucket,
    ctx.params.Key,
    uploadId,
    ctx.headers,
  );
  ctx.body = {
    InitiateMultipartUploadResult: {
      Bucket: ctx.params.bucket,
      Key: ctx.params.Key,
      UploadId: uploadId,
    },
  };
};

/**
 * Upload Part
 * This operation uploads a part in a multipart upload. Part numbers can be any number from 1 to
 * 10,000, inclusive. A part number uniquely identifies a part and also defines its position within
 * the object being created. If you upload a new part using the same part number that was used with
 * a previous part, the previously uploaded part is overwritten. Each part must be at least 5 MB in
 * size, except the last part. There is no size limit on the last part of your multipart upload.
 * {@link https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html}
 */
exports.uploadPart = async function uploadPart(ctx) {
  aws4SignatureBodyParser(ctx);
  try {
    const { md5 } = await ctx.store.putPart(
      ctx.params.bucket,
      ctx.query.uploadId,
      ctx.query.partNumber,
      ctx.request.body,
    );
    ctx.logger.info(
      'Stored part %s of %s in bucket "%s" successfully',
      ctx.query.partNumber,
      ctx.query.uploadId,
      ctx.params.bucket,
    );
    ctx.etag = md5;
    ctx.body = '';
  } catch (err) {
    ctx.logger.error(
      'Error uploading part %s of %s to bucket "%s"',
      ctx.query.partNumber,
      ctx.query.uploadId,
      ctx.params.bucket,
      err,
    );
    throw err;
  }
};

exports.uploadPartCopy = async function uploadPartCopy(ctx) {
  let copySource = decodeURI(ctx.headers['x-amz-copy-source']);
  copySource = copySource.startsWith('/') ? copySource.slice(1) : copySource;
  let [srcBucket, ...srcKey] = copySource.split('/');
  srcKey = srcKey.join('/');

  const bucket = await ctx.store.getBucket(srcBucket);
  if (!bucket) {
    ctx.logger.error('No bucket found for "%s"', srcBucket);
    throw new S3Error('NoSuchBucket', 'The specified bucket does not exist', {
      BucketName: srcBucket,
    });
  }

  const options = {};
  if ('x-amz-copy-source-range' in ctx.headers) {
    const match = /^bytes=(\d+)-(\d+)$/.exec(
      ctx.headers['x-amz-copy-source-range'],
    );
    if (!match) {
      throw new S3Error(
        'InvalidArgument',
        'The x-amz-copy-source-range value must be of the form bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy',
        {
          ArgumentName: 'x-amz-copy-source-range',
          ArgumentValue: ctx.get('x-amz-copy-source-range'),
        },
      );
    }
    options.start = Number(match[1]);
    options.end = Number(match[2]);
  }

  const object = await ctx.store.getObject(srcBucket, srcKey, options);
  if (!object) {
    throw new S3Error('NoSuchKey', 'The specified Key does not exist.', {
      Key: srcKey,
    });
  }

  // Range request was out of range
  if (object.range && (!object.content || object.range.end < options.end)) {
    if (object.content) object.content.destroy();
    throw new S3Error(
      'InvalidArgument',
      `Range specified is not valid for source object of size: ${object.size}`,
      {
        ArgumentName: 'x-amz-copy-source-range',
        ArgumentValue: ctx.get('x-amz-copy-source-range'),
      },
    );
  }

  try {
    const { md5 } = await ctx.store.putPart(
      ctx.params.bucket,
      ctx.query.uploadId,
      ctx.query.partNumber,
      object.content,
    );
    ctx.logger.info(
      'Copied part %s of %s in bucket "%s" successfully',
      ctx.query.partNumber,
      ctx.query.uploadId,
      ctx.params.bucket,
    );
    ctx.etag = md5;
    ctx.body = {
      CopyPartResult: {
        LastModified: new Date().toISOString(),
        ETag: JSON.stringify(md5),
      },
    };
  } catch (err) {
    ctx.logger.error(
      'Error copying part %s of %s to bucket "%s"',
      ctx.query.partNumber,
      ctx.query.uploadId,
      ctx.params.bucket,
      err,
    );
    throw err;
  }
};
