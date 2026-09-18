package blob

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var ownedKey = regexp.MustCompile(`^objects/[0-9a-f]{64}$`)

type Sweep struct{ Versions, Bytes int64 }
type Registered func(context.Context, []string, []string) ([]bool, error)

// PruneUnregistered deletes only explicit versions in our immutable namespace.
// The caller must keep all publishers stopped for the entire operation. Unknown
// keys, unversioned data, delete markers and other buckets are never collected.
func (s *Store) PruneUnregistered(ctx context.Context, registered Registered) (Sweep, error) {
	var result Sweep
	pages := s3.NewListObjectVersionsPaginator(s.client, &s3.ListObjectVersionsInput{
		Bucket: aws.String(DataBucket), Prefix: aws.String("objects/"), MaxKeys: aws.Int32(500),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return result, err
		}
		keys, versions, sizes := collectVersions(page)
		if len(keys) == 0 {
			continue
		}
		deleted, bytes, err := s.deleteUnregistered(ctx, registered, keys, versions, sizes)
		if err != nil {
			return result, err
		}
		result.Versions += deleted
		result.Bytes += bytes
	}
	return result, nil
}

func collectVersions(page *s3.ListObjectVersionsOutput) ([]string, []string, []int64) {
	keys, versions, sizes := []string{}, []string{}, []int64{}
	for _, v := range page.Versions {
		key, version := aws.ToString(v.Key), aws.ToString(v.VersionId)
		if !ownedKey.MatchString(key) || version == "" || version == "null" || v.Size == nil || *v.Size < 0 {
			continue
		}
		keys, versions, sizes = append(keys, key), append(versions, version), append(sizes, *v.Size)
	}
	return keys, versions, sizes
}

func (s *Store) deleteUnregistered(
	ctx context.Context,
	registered Registered,
	keys, versions []string,
	sizes []int64,
) (int64, int64, error) {
	keep, err := registered(ctx, keys, versions)
	if err != nil {
		return 0, 0, err
	}
	if len(keep) != len(keys) {
		return 0, 0, errors.New("incomplete object reference verification")
	}
	deletions := []types.ObjectIdentifier{}
	bytes := int64(0)
	for i, retain := range keep {
		if retain {
			continue
		}
		deletions = append(
			deletions,
			types.ObjectIdentifier{Key: aws.String(keys[i]), VersionId: aws.String(versions[i])},
		)
		bytes += sizes[i]
	}
	if len(deletions) == 0 {
		return 0, 0, nil
	}
	out, err := s.client.DeleteObjects(
		ctx,
		&s3.DeleteObjectsInput{
			Bucket: aws.String(DataBucket),
			Delete: &types.Delete{Objects: deletions, Quiet: aws.Bool(false)},
		},
	)
	if err != nil {
		return 0, 0, err
	}
	if len(out.Errors) > 0 || len(out.Deleted) != len(deletions) {
		return 0, 0, fmt.Errorf(
			"object version deletion was partially acknowledged; retry collection (failures=%d acknowledged=%d requested=%d)",
			len(out.Errors),
			len(out.Deleted),
			len(deletions),
		)
	}
	return int64(len(deletions)), bytes, nil
}
