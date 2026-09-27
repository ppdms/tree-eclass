// Package blob implements immutable, version-pinned S3 document storage.
package blob

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"tree-eclass/internal/domain/objects"
)

const DataBucket = "tree-eclass-data"
const CacheBucket = "tree-eclass-cache"

// Reference and MaxSourceBytes are the shared domain contract from
// domain/objects; they remain identical types for every caller.
type Reference = objects.Reference

const MaxSourceBytes = objects.MaxSourceBytes

type Store struct {
	client  *s3.Client
	uploads chan struct{}
}

func New(endpoint, access, secret string) (*Store, error) {
	if endpoint == "" || access == "" || secret == "" {
		return nil, errors.New("S3 endpoint and credentials are required")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 6
	transport.MaxIdleConnsPerHost = 4
	transport.ResponseHeaderTimeout = 30 * time.Second
	client := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(endpoint), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider(access, secret, ""),
		HTTPClient:  &http.Client{Transport: transport}, RetryMaxAttempts: 3,
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired})
	return &Store{client, make(chan struct{}, 1)}, nil
}

func (s *Store) Setup(ctx context.Context) error {
	for _, bucket := range []string{DataBucket, CacheBucket} {
		_, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		if err != nil && !isCode(err, "BucketAlreadyOwnedByYou") && !isCode(err, "BucketAlreadyExists") {
			return err
		}
	}
	_, err := s.client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String(DataBucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}})
	if err != nil {
		return err
	}
	_, err = s.client.PutBucketLifecycleConfiguration(
		ctx,
		&s3.PutBucketLifecycleConfigurationInput{Bucket: aws.String(CacheBucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{
				Rules: []types.LifecycleRule{{ID: aws.String("disposable-cache"), Status: types.ExpirationStatusEnabled,
					Filter: &types.LifecycleRuleFilter{
						Prefix: aws.String(""),
					}, Expiration: &types.LifecycleExpiration{Days: aws.Int32(7)},
					AbortIncompleteMultipartUpload: &types.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: aws.Int32(1),
					}}},
			}},
	)
	return err
}

func isCode(err error, code string) bool {
	var api smithy.APIError
	return errors.As(err, &api) && api.ErrorCode() == code
}

// Put spools to a bounded temporary file, computes a real SHA-256, and streams
// one conditional upload. No document-sized allocation or multipart ETag hash.
func (s *Store) Put(ctx context.Context, input io.Reader, mediaType, tempDir string) (Reference, error) {
	select {
	case s.uploads <- struct{}{}:
		defer func() { <-s.uploads }()
	case <-ctx.Done():
		return Reference{}, ctx.Err()
	}
	f, err := os.CreateTemp(tempDir, "upload-*")
	if err != nil {
		return Reference{}, err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	hash := sha256.New()
	size, err := io.Copy(io.MultiWriter(f, hash), io.LimitReader(input, MaxSourceBytes+1))
	if err != nil {
		return Reference{}, err
	}
	if size == 0 || size > MaxSourceBytes {
		return Reference{}, errors.New("document must contain between 1 byte and 50 MiB")
	}
	digest := fmt.Sprintf("%x", hash.Sum(nil))
	ref := Reference{
		Bucket: DataBucket, Key: "objects/" + digest, VersionID: digest,
		SHA256: digest, Bytes: size, MediaType: mediaType,
	}
	ref.VersionID = ""
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return ref, err
	}
	out, err := s.client.PutObject(
		ctx,
		&s3.PutObjectInput{Bucket: aws.String(ref.Bucket), Key: aws.String(ref.Key), Body: f,
			ContentLength: aws.Int64(
				size,
			), ContentType: aws.String(mediaType), IfNoneMatch: aws.String("*"), Metadata: map[string]string{"sha256": digest}},
	)
	if isCode(err, "PreconditionFailed") {
		return s.existing(ctx, ref)
	}
	if err != nil {
		return ref, err
	}
	ref.VersionID = aws.ToString(out.VersionId)
	if ref.VersionID == "" || ref.VersionID == "null" {
		return ref, errors.New("durable bucket did not return an object version")
	}
	return s.verify(ctx, ref)
}

func (s *Store) existing(ctx context.Context, ref Reference) (Reference, error) {
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(ref.Bucket), Key: aws.String(ref.Key)})
	if err != nil {
		return ref, err
	}
	ref.VersionID = aws.ToString(head.VersionId)
	return s.verify(ctx, ref)
}

func (s *Store) verify(ctx context.Context, ref Reference) (Reference, error) {
	if ref.VersionID == "" || ref.VersionID == "null" {
		return ref, errors.New("unversioned durable object")
	}
	head, err := s.client.HeadObject(
		ctx,
		&s3.HeadObjectInput{
			Bucket:    aws.String(ref.Bucket),
			Key:       aws.String(ref.Key),
			VersionId: aws.String(ref.VersionID),
		},
	)
	if err != nil {
		return ref, err
	}
	if aws.ToInt64(head.ContentLength) != ref.Bytes || head.Metadata["sha256"] != ref.SHA256 {
		return ref, errors.New("uploaded object verification failed")
	}
	ref.MediaType = aws.ToString(head.ContentType)
	return ref, nil
}

func (s *Store) Get(ctx context.Context, ref Reference, byteRange string) (*s3.GetObjectOutput, error) {
	if ref.VersionID == "" {
		return nil, errors.New("object version is required")
	}
	input := &s3.GetObjectInput{
		Bucket:    aws.String(ref.Bucket),
		Key:       aws.String(ref.Key),
		VersionId: aws.String(ref.VersionID),
	}
	if byteRange != "" {
		input.Range = aws.String(byteRange)
	}
	return s.client.GetObject(ctx, input)
}

func (s *Store) Open(ctx context.Context, ref Reference) (io.ReadCloser, error) {
	out, err := s.Get(ctx, ref, "")
	if err != nil {
		return nil, err
	}
	if out.Body == nil {
		return nil, errors.New("object response has no body")
	}
	return out.Body, nil
}
