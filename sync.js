import 'dotenv/config';
import fetch from 'node-fetch';

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
    for (let i = 0; i < chunks; i++) {
        console.time(`chunk ${i}/${chunks}`);
        const start = i * multipartLimit;
        const end = Math.min(contentLength, (i + 1) * multipartLimit) - 1;
        console.log(`* fetch chunk ${i}/${chunks}: ${start}-${end}`);

        const resp = await fetch(srcPmt, {
            headers: {
                range: `bytes=${start}-${end}`
            }
        });

        console.log(`* uploading chunk ${i}/${chunks}: ${start}-${end}`);
        const s3Req = new UploadPartCommand({
            Bucket: process.env.BUCKET_NAME,
            Key: key,
            PartNumber: i + 1,
            UploadId: multiPartUpload.UploadId,
            Body: resp.body,
            ContentLength: end - start + 1
        });
        const uploadedPart = await S3.send(s3Req);

        console.log(`* uploaded ${i}/${chunks}: ${start}-${end}`);
        parts.push({ ETag: uploadedPart.ETag, PartNumber: i + 1 });
        console.timeEnd(`chunk ${i}/${chunks}`);
    }

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