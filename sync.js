import 'dotenv/config';
import fetch from 'node-fetch';
import * as async from 'async';

import { S3Client, CreateMultipartUploadCommand, CompleteMultipartUploadCommand, UploadPartCommand } from "@aws-sdk/client-s3";

const S3 = new S3Client({
    region: "auto",
    endpoint: `https://${process.env.ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
        accessKeyId: process.env.ACCESS_KEY_ID,
        secretAccessKey: process.env.SECRET_ACCESS_KEY,
    },
});

async function sync() {
    const meta = await (await fetch(process.env.META_PATH)).json();
    const lastMap = meta[meta.length - 1];
    const key = `oss/planet/${lastMap.uploaded.substr(0, 10)}.pmt`;
    const srcPmt = process.env.ROOT_PATH + lastMap.key;

    const multipartLimit = 1024 * 1024 * 1024; // 1GB chunks
    const headResp = await fetch(srcPmt, {
        method: 'HEAD'
    });

    if (!headResp.ok) throw new Error(`unexpected response ${headResp.statusText}`);

    const contentLength = parseInt(headResp.headers.get('content-length'));
    const chunks = Math.ceil(contentLength / multipartLimit);

    console.log(`Total size is ${contentLength} bytes (${(contentLength / 1024 / 1024 / 1024).toFixed(4)}GB)`);
    console.log(`Chunks ${chunks}`);
    console.log({ key, contentLength, chunks });

    console.time(`multipart`);
    const multiPartUpload = await S3.send(new CreateMultipartUploadCommand({
        Bucket: process.env.BUCKET_NAME,
        Key: key
    }));

    console.log(`Upload ID: ${multiPartUpload.UploadId}`);

    let parts = [];
    let reqParts = [];
    for (let i = 0; i < chunks; i++) {
        const start = i * multipartLimit;
        const end = Math.min(contentLength, (i + 1) * multipartLimit) - 1;

        reqParts.push({
            i,
            reqHeaders: {
                range: `bytes=${start}-${end}`
            },
            s3Params: {
                PartNumber: i + 1,
                ContentLength: end - start + 1
            }
        })
    }

    await async.mapLimit(reqParts, 10, async ({ i, reqHeaders, s3Params }) => {
        console.time(`chunk ${i}/${chunks}`);
        console.log(`* fetch chunk ${i}/${chunks}: ${reqHeaders.range}`);

        const resp = await fetch(srcPmt, {
            headers: reqHeaders
        });

        console.log(`* uploading chunk ${i}/${chunks}: ${reqHeaders.range}`);
        const s3Req = new UploadPartCommand({
            Bucket: process.env.BUCKET_NAME,
            Key: key,
            UploadId: multiPartUpload.UploadId,
            Body: resp.body,
            ...s3Params
        });
        const uploadedPart = await S3.send(s3Req);

        console.log(`* uploaded ${i}/${chunks}: ${reqHeaders.range}`);
        parts.push({ ETag: uploadedPart.ETag, PartNumber: i + 1 });
        console.timeEnd(`chunk ${i}/${chunks}`);
    });

    console.log(parts);
    console.log(`completing...`);

    const resp = await S3.send(new CompleteMultipartUploadCommand({
        Bucket: process.env.BUCKET_NAME,
        Key: key,
        UploadId: multiPartUpload.UploadId,
        MultipartUpload: {
            Parts: parts
        }
    }))

    console.log(`completed`, resp);
    console.timeEnd(`multipart`);
}

sync()