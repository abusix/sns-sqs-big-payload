import { MessageAttributeMap } from 'aws-sdk/clients/sns';
import { PayloadMeta, S3PayloadMeta, SqsExtendedPayloadMeta } from './types';
import { AMAZON_EXTENDED_CLIENT_PAYLOAD_OFFLOADING_REFERENCE, SQS_LARGE_PAYLOAD_SIZE_ATTRIBUTE } from './constants';

export function createExtendedCompatibilityAttributeMap(
    msgSize: number,
    base?: MessageAttributeMap,
): MessageAttributeMap {
    const result = base ? base : {};
    result[SQS_LARGE_PAYLOAD_SIZE_ATTRIBUTE] = {
        StringValue: '' + msgSize,
        DataType: 'Number',
    };
    return result;
}

export function buildS3Payload(s3PayloadMeta: S3PayloadMeta): string {
    return JSON.stringify({
        S3Payload: s3PayloadMeta,
    } as PayloadMeta);
}

export function buildS3PayloadWithExtendedCompatibility(s3PayloadMeta: S3PayloadMeta): string {
    return JSON.stringify(
        [AMAZON_EXTENDED_CLIENT_PAYLOAD_OFFLOADING_REFERENCE, {
            s3BucketName: s3PayloadMeta.Bucket,
            s3Key: s3PayloadMeta.Key,
        }] as SqsExtendedPayloadMeta);
}

export function computeMsgAttributeSize(attributes: MessageAttributeMap): number {
    if (!attributes) {
        return 0;
    }
    let totalSize = 0;
    for (const [k, v] of Object.entries(attributes)) {
        totalSize += Buffer.byteLength(k);
        totalSize += Buffer.byteLength(v.DataType);
        if (v.BinaryValue) {
            // Blob is basically everything. Try to measure with Buffer.byteLength, otherwise use JSON.stringify
            try {
                // @ts-ignore
                totalSize += Buffer.byteLength(v.BinaryValue);
            } catch {
                totalSize += Buffer.byteLength(JSON.stringify(v.BinaryValue));
            }
        }
        if (v.StringValue) {
            totalSize += Buffer.byteLength(v.StringValue);
        }
    }
    return totalSize;
}