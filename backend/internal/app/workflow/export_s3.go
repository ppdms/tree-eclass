package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/platform"
	"tree-eclass/internal/infrastructure/blob"
)

// ExportS3 migrates a legacy S3-backed dataset into the filesystem object
// store. It is the one-shot bridge for datasets created before object storage
// moved onto the local filesystem; the AWS SDK remains in the build only for
// this command.
func (c *Controller) ExportS3(ctx context.Context, args []string) (err error) {
	if err = c.configured(); err != nil {
		return err
	}
	if err = c.stopped(); err != nil {
		return err
	}
	flags, err := parseExportFlags(args)
	if err != nil {
		return err
	}
	endpoint, access, secret, err := c.legacyS3Target(flags.endpoint, flags.access, flags.secret)
	if err != nil {
		return err
	}
	out := flags.out
	if out == "" {
		out = c.objectsRoot()
	}
	if err = os.MkdirAll(out, 0700); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, c.stopAll()) }()
	if err = c.startPostgres(ctx); err != nil {
		return err
	}
	if err = c.waitDatabase(ctx); err != nil {
		return err
	}
	rows, err := c.registeredObjects(ctx)
	if err != nil {
		return err
	}
	client := s3.New(s3.Options{
		Region: "us-east-1", BaseEndpoint: aws.String(endpoint), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider(access, secret, ""),
	})
	exported, existing, failures := exportObjects(ctx, client, out, rows)
	fmt.Printf(
		"storage export-s3: %d registered objects; %d exported, %d already present, %d failed into %s\n",
		len(rows), exported, existing, len(rows)-exported-existing, out,
	)
	if failures != nil {
		return fmt.Errorf("export incomplete: %w", failures)
	}
	return nil
}

type legacyObject struct {
	key    string
	sha256 string
	bytes  int64
}

// registeredObjects lists every object row the catalog still references, so a
// lossless export can be verified against the database rather than a bucket
// listing.
func (c *Controller) registeredObjects(ctx context.Context) ([]legacyObject, error) {
	conn, err := pgx.Connect(ctx, c.databaseURL())
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx,
		"SELECT DISTINCT key, sha256, bytes FROM app.objects WHERE bucket='tree-eclass-data'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	objects := []legacyObject{}
	for rows.Next() {
		var o legacyObject
		if err = rows.Scan(&o.key, &o.sha256, &o.bytes); err != nil {
			return nil, err
		}
		objects = append(objects, o)
	}
	return objects, rows.Err()
}

func exportObjects(
	ctx context.Context, client *s3.Client, out string, rows []legacyObject,
) (int, int, error) {
	exported, existing := 0, 0
	var failures []error
	for _, row := range rows {
		digest, err := digestFromKey(row.key)
		if err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", row.key, err))
			continue
		}
		wrote, err := exportObject(ctx, client, out, row.key, digest, row.bytes)
		if err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", row.key, err))
			continue
		}
		if wrote {
			exported++
		} else {
			existing++
		}
	}
	return exported, existing, errors.Join(failures...)
}

// exportObject fetches the latest version of one key, verifies its digest and
// size against the catalog, and atomically publishes it as <out>/<sha256>.
func exportObject(
	ctx context.Context, client *s3.Client, out, key, digest string, size int64,
) (bool, error) {
	target := filepath.Join(out, digest)
	if info, err := os.Stat(target); err == nil {
		if info.Size() == size && fileDigest(target) == digest {
			return false, nil
		}
		return false, fmt.Errorf("existing file conflicts with the registered object")
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	get, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(blob.DataBucket), Key: aws.String(key),
	})
	if err != nil {
		return false, err
	}
	defer get.Body.Close()
	temp, err := os.CreateTemp(out, ".export-*")
	if err != nil {
		return false, err
	}
	defer os.Remove(temp.Name())
	hash := sha256.New()
	written, err := io.Copy(io.MultiWriter(temp, hash), get.Body)
	if closeErr := temp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return false, err
	}
	fetched := hex.EncodeToString(hash.Sum(nil))
	if written != size || fetched != digest {
		return false, fmt.Errorf(
			"content mismatch: catalog registers %d bytes as sha256 %s, fetched %d bytes as sha256 %s",
			size, digest, written, fetched,
		)
	}
	if err = os.Rename(temp.Name(), target); err != nil {
		return false, err
	}
	return true, platform.SyncDir(out)
}

// digestFromKey validates the content-addressed key layout objects/<sha256>.
func digestFromKey(key string) (string, error) {
	digest, ok := strings.CutPrefix(key, "objects/")
	if !ok || len(digest) != 64 {
		return "", errors.New("unexpected object key layout")
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return "", fmt.Errorf("object key is not a sha256 digest: %w", err)
	}
	return digest, nil
}

func fileDigest(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	hash := sha256.New()
	if _, err = io.Copy(hash, f); err != nil {
		return ""
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// legacyS3Target resolves the S3 endpoint and credentials for the migration:
// explicit flags win, and missing values fall back to the previous generation
// config.json, whose schema is no longer parsed into Config.
func (c *Controller) legacyS3Target(endpoint, access, secret string) (string, string, string, error) {
	if endpoint != "" && access != "" && secret != "" {
		return endpoint, access, secret, nil
	}
	raw, err := legacyConfig(filepath.Join(c.Root, "config.json"))
	if err != nil {
		return "", "", "", err
	}
	if endpoint == "" {
		if port := legacyNumber(raw, "ports", "s3"); port > 0 {
			endpoint = fmt.Sprintf("http://127.0.0.1:%d", int(port))
		}
	}
	if access == "" {
		access = legacyString(raw, "s3_access")
	}
	if secret == "" {
		secret = legacyString(raw, "s3_secret")
	}
	if endpoint == "" || access == "" || secret == "" {
		return "", "", "", errors.New(
			"legacy S3 target is incomplete; pass --endpoint, --access and --secret",
		)
	}
	return endpoint, access, secret, nil
}

func legacyConfig(path string) (map[string]any, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, errors.New("no legacy configuration found; pass --endpoint, --access and --secret")
	}
	if err != nil {
		return nil, err
	}
	var raw map[string]any
	if err = json.Unmarshal(b, &raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func legacyString(raw map[string]any, name string) string {
	value, _ := raw[name].(string)
	return value
}

func legacyNumber(raw map[string]any, section, name string) float64 {
	group, _ := raw[section].(map[string]any)
	value, _ := group[name].(float64)
	return value
}

// parseExportFlags accepts --name value and --name=value for the four
// migration options.
func parseExportFlags(args []string) (exportFlags, error) {
	flags := exportFlags{}
	values := map[string]*string{
		"endpoint": &flags.endpoint, "access": &flags.access,
		"secret": &flags.secret, "out": &flags.out,
	}
	const usage = "usage: storage export-s3 [--endpoint URL] [--access KEY] [--secret KEY] [--out DIR]"
	for i := 0; i < len(args); i++ {
		if !strings.HasPrefix(args[i], "--") {
			return flags, fmt.Errorf("unexpected argument %q; %s", args[i], usage)
		}
		name, value, inline := strings.Cut(strings.TrimPrefix(args[i], "--"), "=")
		target, ok := values[name]
		if !ok {
			return flags, fmt.Errorf("unknown flag --%s; %s", name, usage)
		}
		if !inline {
			if i+1 >= len(args) {
				return flags, fmt.Errorf("flag --%s requires a value; %s", name, usage)
			}
			i++
			value = args[i]
		}
		*target = value
	}
	return flags, nil
}

type exportFlags struct{ endpoint, access, secret, out string }
