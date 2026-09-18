package blob

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Check reads the storage contract without creating probe objects or buckets.
func (s *Store) Check(ctx context.Context) error {
	for _, bucket := range []string{DataBucket, CacheBucket} {
		if _, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err != nil {
			return err
		}
	}
	versioning, err := s.client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(DataBucket)})
	if err != nil {
		return err
	}
	if versioning.Status != types.BucketVersioningStatusEnabled {
		return errors.New("document bucket versioning is disabled")
	}
	return nil
}
