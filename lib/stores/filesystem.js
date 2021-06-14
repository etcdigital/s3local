'use strict';

const crypto = require('crypto');
const {
  createReadStream,
  createWriteStream,
  rmdirSync,
  readdirSync,
  promises: fs,
} = require('fs');
const { pipeline, Transform } = require('stream');
const { pick, pickBy, sortBy, zip } = require('lodash');
const path = require('path');
const { format } = require('util');

const { getConfigModel } = require('../models/config');
const S3Bucket = require('../models/bucket');
const S3Object = require('../models/object');
const { concatStreams, walk } = require('../utils');

const S3RVER_SUFFIX = '%s._S3rver_%s';

class FilesystemStore {
  static decodeKeyPath(KeyPath) {
    return process.platform === 'win32'
      ? KeyPath.replace(/&../g, (ent) =>
          Buffer.from(ent.slice(1), 'hex').toString(),
        )
      : KeyPath;
  }

  static encodeKeyPath(Key) {
    return process.platform === 'win32'
      ? Key.replace(
          /[<>:"\\|?*]/g,
          (ch) => '&' + Buffer.from(ch, 'utf8').toString('hex'),
        )
      : Key;
  }

  constructor(rootDirectory) {
    this.rootDirectory = rootDirectory;
  }

  // helpers

  getBucketPath(bucketName) {
    return path.join(this.rootDirectory, bucketName);
  }

  getResourcePath(bucket, Key = '', resource) {
    const parts = FilesystemStore.encodeKeyPath(Key).split('/');
    const suffix = format(S3RVER_SUFFIX, parts.pop(), resource);
    return path.join(this.rootDirectory, bucket, ...parts, suffix);
  }

  async getMetadata(bucket, Key) {
    const objectPath = this.getResourcePath(bucket, Key, 'object');
    const metadataPath = this.getResourcePath(bucket, Key, 'metadata.json');

    // this is expected to throw if the object doesn't exist
    const stat = await fs.stat(objectPath);
    const [storedMetadata, md5] = await Promise.all([
      fs
        .readFile(metadataPath)
        .then(JSON.parse)
        .catch((err) => {
          if (err.code === 'ENOENT') return undefined;
          throw err;
        }),
      fs
        .readFile(`${objectPath}.md5`)
        .then((md5) => md5.toString())
        .catch(async (err) => {
          if (err.code !== 'ENOENT') throw err;
          // create the md5 file if it doesn't already exist
          const md5 = await new Promise((resolve, reject) => {
            const stream = createReadStream(objectPath);
            const md5Context = crypto.createHash('md5');
            stream.on('error', reject);
            stream.on('data', (chunk) => md5Context.update(chunk, 'utf8'));
            stream.on('end', () => resolve(md5Context.digest('hex')));
          });
          await fs.writeFile(`${objectPath}.md5`, md5);
          return md5;
        }),
    ]);

    return {
      ...storedMetadata,
      etag: JSON.stringify(md5),
      'last-modified': stat.mtime.toUTCString(),
      'content-length': stat.size,
    };
  }

  async putMetadata(bucket, Key, metadata, md5) {
    const metadataPath = this.getResourcePath(bucket, Key, 'metadata.json');
    const md5Path = this.getResourcePath(bucket, Key, 'object.md5');

    const json = {
      ...pick(metadata, S3Object.ALLOWED_METADATA),
      ...pickBy(metadata, (value, Key) => Key.startsWith('x-amz-meta-')),
    };

    if (md5) await fs.writeFile(md5Path, md5);
    await fs.writeFile(metadataPath, JSON.stringify(json, null, 2));
  }

  // store implementation

  reset() {
    const list = readdirSync(this.rootDirectory);
    for (const file of list) {
      rmdirSync(path.join(this.rootDirectory, file), { recursive: true });
    }
  }

  async listBuckets() {
    const list = await fs.readdir(this.rootDirectory);
    const buckets = await Promise.all(
      list.map((filename) => this.getBucket(filename)),
    );
    return buckets.filter(Boolean);
  }

  async getBucket(bucket) {
    const bucketPath = this.getBucketPath(bucket);
    try {
      const stat = await fs.stat(bucketPath);
      if (!stat.isDirectory()) return null;
      return new S3Bucket(bucket, stat.birthtime);
    } catch (err) {
      if (err.code === 'ENOENT') return null;
      throw err;
    }
  }

  async putBucket(bucket) {
    const bucketPath = this.getBucketPath(bucket);
    await fs.mkdir(bucketPath, 0o0755, { recursive: true });
    return this.getBucket(bucket);
  }

  async deleteBucket(bucket) {
    return fs.rm(this.getBucketPath(bucket), { recursive: true });
  }

  async listObjects(bucket, options) {
    const {
      delimiter = '',
      startAfter = '',
      prefix = '',
      maxKeys = Infinity,
    } = options;
    const bucketPath = this.getBucketPath(bucket);

    const delimiterEnc = FilesystemStore.encodeKeyPath(delimiter);
    const startAfterPath = [
      bucketPath,
      FilesystemStore.encodeKeyPath(startAfter),
    ].join('/');
    const prefixPath = [bucketPath, FilesystemStore.encodeKeyPath(prefix)].join(
      '/',
    );

    const it = walk(bucketPath, (dirPath) => {
      // avoid directories occurring before the startAfter parameter
      if (dirPath < startAfterPath && startAfterPath.indexOf(dirPath) === -1) {
        return false;
      }
      if (dirPath.startsWith(prefixPath)) {
        if (
          delimiterEnc &&
          dirPath.slice(prefixPath.length).indexOf(delimiterEnc) !== -1
        ) {
          // avoid directories occurring beneath a common prefix
          return false;
        }
      } else if (!prefixPath.startsWith(dirPath)) {
        // avoid directories that do not intersect with any part of the prefix
        return false;
      }
      return true;
    });

    const commonPrefixes = new Set();
    const objectSuffix = format(S3RVER_SUFFIX, '', 'object');
    const Keys = [];
    let isTruncated = false;
    for (const KeyPath of it) {
      if (!KeyPath.endsWith(objectSuffix)) {
        continue;
      }

      const Key = FilesystemStore.decodeKeyPath(
        KeyPath.slice(bucketPath.length + 1, -objectSuffix.length),
      );

      if (Key <= startAfter || !Key.startsWith(prefix)) {
        continue;
      }

      if (delimiter) {
        const idx = Key.slice(prefix.length).indexOf(delimiter);
        if (idx !== -1) {
          // add to common prefixes before filtering this Key out
          commonPrefixes.add(Key.slice(0, prefix.length + idx + 1));
          continue;
        }
      }

      if (Keys.length < maxKeys) {
        Keys.push(Key);
      } else {
        isTruncated = true;
        break;
      }
    }

    const metadataArr = await Promise.all(
      Keys.map((Key) =>
        this.getMetadata(bucket, Key).catch((err) => {
          if (err.code === 'ENOENT') return undefined;
          throw err;
        }),
      ),
    );

    return {
      objects: zip(Keys, metadataArr)
        .filter(([, metadata]) => metadata !== undefined)
        .map(([Key, metadata]) => new S3Object(bucket, Key, null, metadata)),
      commonPrefixes: [...commonPrefixes].sort(),
      isTruncated,
    };
  }

  async existsObject(bucket, Key) {
    const objectPath = this.getResourcePath(bucket, Key, 'object');
    try {
      await fs.stat(objectPath);
      return true;
    } catch (err) {
      if (err.code === 'ENOENT') return false;
      throw err;
    }
  }

  async getObject(bucket, Key, options) {
    try {
      const metadata = await this.getMetadata(bucket, Key);
      const lastByte = Math.max(0, Number(metadata['content-length']) - 1);
      const range = {
        start: (options && options.start) || 0,
        end: Math.min((options && options.end) || Infinity, lastByte),
      };

      if (range.start < 0 || Math.min(range.end, lastByte) < range.start) {
        // the range is not satisfiable
        const object = new S3Object(bucket, Key, null, metadata);
        if (options && (options.start || options.end)) {
          object.range = range;
        }
        return object;
      }

      const content = await new Promise((resolve, reject) => {
        const stream = createReadStream(
          this.getResourcePath(bucket, Key, 'object'),
          range,
        )
          .on('error', reject)
          .on('open', () => resolve(stream));
      });
      const object = new S3Object(bucket, Key, content, metadata);
      if (options && (options.start || options.end)) {
        object.range = range;
      }
      return object;
    } catch (err) {
      if (err.code === 'ENOENT') return null;
      throw err;
    }
  }

  async putObject(object) {
    const objectPath = this.getResourcePath(
      object.bucket,
      object.Key,
      'object',
    );

    await fs.mkdir(path.dirname(objectPath), { recursive: true });

    const [size, md5] = await new Promise((resolve, reject) => {
      const writeStream = createWriteStream(objectPath);
      const md5Context = crypto.createHash('md5');

      if (Buffer.isBuffer(object.content)) {
        writeStream.end(object.content);
        md5Context.update(object.content);
        resolve([object.content.length, md5Context.digest('hex')]);
      } else {
        let totalLength = 0;
        pipeline(
          object.content,
          new Transform({
            transform(chunk, encoding, callback) {
              md5Context.update(chunk, encoding);
              totalLength += chunk.length;
              callback(null, chunk);
            },
          }),
          writeStream,
          (err) =>
            err
              ? reject(err)
              : resolve([totalLength, md5Context.digest('hex')]),
        );
      }
    });
    await this.putMetadata(object.bucket, object.Key, object.metadata, md5);
    return { size, md5 };
  }

  async copyObject(
    srcBucket,
    srcKey,
    destBucket,
    destKey,
    replacementMetadata,
  ) {
    const srcObjectPath = this.getResourcePath(srcBucket, srcKey, 'object');
    const destObjectPath = this.getResourcePath(destBucket, destKey, 'object');

    if (srcObjectPath !== destObjectPath) {
      await fs.mkdir(path.dirname(destObjectPath), { recursive: true });
      await fs.copyFile(srcObjectPath, destObjectPath);
    }

    if (replacementMetadata) {
      await this.putMetadata(destBucket, destKey, replacementMetadata);
      return this.getMetadata(destBucket, destKey);
    } else {
      if (srcObjectPath !== destObjectPath) {
        await Promise.all([
          fs.copyFile(
            this.getResourcePath(srcBucket, srcKey, 'metadata.json'),
            this.getResourcePath(destBucket, destKey, 'metadata.json'),
          ),
          fs.copyFile(
            this.getResourcePath(srcBucket, srcKey, 'object.md5'),
            this.getResourcePath(destBucket, destKey, 'object.md5'),
          ),
        ]);
      }
      return this.getMetadata(destBucket, destKey);
    }
  }

  async deleteObject(bucket, Key) {
    await Promise.all(
      [
        this.getResourcePath(bucket, Key, 'object'),
        this.getResourcePath(bucket, Key, 'object.md5'),
        this.getResourcePath(bucket, Key, 'metadata.json'),
      ].map((filePath) =>
        fs.unlink(filePath).catch((err) => {
          if (err.code !== 'ENOENT') throw err;
        }),
      ),
    );
    // clean up empty directories
    const bucketPath = this.getBucketPath(bucket);
    const parts = Key.split('/');
    // the last part isn't a directory (it's embedded into the file name)
    parts.pop();
    while (
      parts.length &&
      !readdirSync(path.join(bucketPath, ...parts)).length
    ) {
      await fs.rm(path.join(bucketPath, ...parts));
      parts.pop();
    }
  }

  async initiateUpload(bucket, Key, uploadId, metadata) {
    const uploadDir = path.join(
      this.getResourcePath(bucket, undefined, 'uploads'),
      uploadId,
    );

    await fs.mkdir(uploadDir, { recursive: true });

    await Promise.all([
      fs.writeFile(path.join(uploadDir, 'Key'), Key),
      fs.writeFile(path.join(uploadDir, 'metadata'), JSON.stringify(metadata)),
    ]);
  }

  async putPart(bucket, uploadId, partNumber, content) {
    const partPath = path.join(
      this.getResourcePath(bucket, undefined, 'uploads'),
      uploadId,
      partNumber.toString(),
    );

    await fs.mkdir(path.dirname(partPath), { recursive: true });

    const [size, md5] = await new Promise((resolve, reject) => {
      const writeStream = createWriteStream(partPath);
      const md5Context = crypto.createHash('md5');
      let totalLength = 0;

      pipeline(
        content,
        new Transform({
          transform(chunk, encoding, callback) {
            md5Context.update(chunk, encoding);
            totalLength += chunk.length;
            callback(null, chunk);
          },
        }),
        writeStream,
        (err) =>
          err ? reject(err) : resolve([totalLength, md5Context.digest('hex')]),
      );
    });
    await fs.writeFile(`${partPath}.md5`, md5);
    return { size, md5 };
  }

  async putObjectMultipart(bucket, uploadId, parts) {
    const uploadDir = path.join(
      this.getResourcePath(bucket, undefined, 'uploads'),
      uploadId,
    );
    const [Key, metadata] = await Promise.all([
      fs.readFile(path.join(uploadDir, 'Key')).then((data) => data.toString()),
      fs.readFile(path.join(uploadDir, 'metadata')).then(JSON.parse),
    ]);
    const partStreams = sortBy(parts, (part) => part.number).map((part) =>
      createReadStream(path.join(uploadDir, part.number.toString())),
    );
    const object = new S3Object(
      bucket,
      Key,
      concatStreams(partStreams),
      metadata,
    );
    const result = await this.putObject(object);
    await fs.rm(uploadDir, { recursive: true });
    return result;
  }

  async getSubresource(bucket, Key, resourceType) {
    const resourcePath = this.getResourcePath(
      bucket,
      Key,
      `${resourceType}.xml`,
    );

    const Model = getConfigModel(resourceType);

    try {
      const data = await fs.readFile(resourcePath);
      return new Model(data.toString());
    } catch (err) {
      if (err.code === 'ENOENT') return null;
      throw err;
    }
  }

  async putSubresource(bucket, Key, resource) {
    const resourcePath = this.getResourcePath(
      bucket,
      Key,
      `${resource.type}.xml`,
    );
    await fs.writeFile(resourcePath, resource.toXML(2));
  }

  async deleteSubresource(bucket, Key, resourceType) {
    const resourcePath = this.getResourcePath(
      bucket,
      Key,
      `${resourceType}.xml`,
    );
    try {
      await fs.unlink(resourcePath);
    } catch (err) {
      if (err.code !== 'ENOENT') throw err;
    }
  }
}

module.exports = FilesystemStore;
