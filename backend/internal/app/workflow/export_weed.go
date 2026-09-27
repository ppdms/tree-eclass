package workflow

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"tree-eclass/internal/domain/platform"
	"tree-eclass/internal/infrastructure/blob"
)

// startLegacyWeed boots the previous-generation SeaweedFS mini cluster in
// place against the existing dataset directory, only for the duration of the
// export. The S3 endpoint must serve the same data directory the legacy stack
// wrote; a fresh temp dir would export nothing.
func (c *Controller) startLegacyWeed(ctx context.Context, endpoint, access, secret string) (func() error, error) {
	noop := func() error { return nil }
	weed := filepath.Join(c.Root, "tools", "seaweedfs-4.46", "weed")
	if _, err := os.Stat(weed); err != nil {
		return noop, fmt.Errorf("legacy SeaweedFS binary is missing at %s: %w", weed, err)
	}
	config := filepath.Join(c.Root, "s3.json")
	identity := map[string]any{
		"identities": []any{
			map[string]any{
				"name":        "tree",
				"credentials": []any{map[string]string{"accessKey": access, "secretKey": secret}},
				"actions":     []string{"Admin", "Read", "Write", "List", "Tagging"},
			},
		},
	}
	if err := platform.WriteJSON(config, identity); err != nil {
		return noop, err
	}
	ports, err := c.legacyPorts()
	if err != nil {
		return noop, err
	}
	dir := filepath.Join(c.active(), "seaweed")
	args := []string{
		weed, "mini",
		"-dir=" + dir,
		"-ip=127.0.0.1", "-ip.bind=127.0.0.1",
		"-master.port=" + ports["master"], "-volume.port=" + ports["volume"],
		"-filer.port=" + ports["filer"], "-s3.port=" + ports["s3"],
		"-admin.port=" + ports["admin"],
		"-admin.dataDir=" + filepath.Join(dir, "admin"),
		"-master.telemetry=false", "-admin.ui=false", "-webdav=false",
		"-s3.port.iceberg=0", "-s3.port.lance=0", "-s3.iam=false",
		"-s3.config=" + config,
		"-s3.autoCreateBucket=false", "-s3.allowDeleteBucketNotEmpty=false",
		"-s3.cacheCapacityMB=0",
		"-master.volumeSizeLimitMB=128", "-volume.max=64", "-volume.index=leveldb",
		"-volume.readBufferSizeMB=1",
		"-filer.concurrentFileUploadLimit=1", "-s3.concurrentFileUploadLimit=1",
		"-s3.concurrentUploadLimitMB=64", "-volume.concurrentUploadLimitMB=64",
		"-volume.concurrentDownloadLimitMB=64",
		"-filer.disableDirListing=true", "-filer.exposeDirectoryData=false",
	}
	if err := c.start(ctx, "seaweed", args, []string{"GOMEMLIMIT=192MiB"}, int(syscall.SIGTERM)); err != nil {
		return noop, err
	}
	stop := func() error { return c.Processes.Stop("seaweed") }
	if err := c.waitLegacyS3(ctx, endpoint, access, secret); err != nil {
		return stop, errors.Join(err, stop())
	}
	return stop, nil
}

// legacyPorts reads the previous-generation port numbers from config.json.
// The running Config no longer carries them; the legacy cluster must bind the
// same ports the dataset's S3 identity expects.
func (c *Controller) legacyPorts() (map[string]string, error) {
	raw, err := legacyConfig(filepath.Join(c.Root, "config.json"))
	if err != nil {
		return nil, err
	}
	ports := map[string]string{}
	for _, name := range []string{"master", "volume", "filer", "s3", "admin"} {
		port := legacyNumber(raw, "ports", name)
		if port <= 0 {
			return nil, fmt.Errorf("legacy config is missing ports.%s; pass --endpoint explicitly", strings.ToLower(name))
		}
		ports[strings.ToLower(name)] = fmt.Sprint(int(port))
	}
	return ports, nil
}

// waitLegacyS3 polls the revived S3 endpoint until the durable bucket answers
// or the deadline passes. HeadBucket needs no object reads.
func (c *Controller) waitLegacyS3(ctx context.Context, endpoint, access, secret string) error {
	client := s3.New(s3.Options{
		Region: "us-east-1", BaseEndpoint: aws.String(endpoint), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider(access, secret, ""),
	})
	deadline := time.Now().Add(30 * time.Second)
	retry := time.NewTimer(0)
	if !retry.Stop() {
		<-retry.C
	}
	defer retry.Stop()
	for {
		if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(blob.DataBucket)}); err == nil {
			return nil
		} else if ctx.Err() != nil {
			return ctx.Err()
		} else if time.Now().After(deadline) {
			return fmt.Errorf("legacy SeaweedFS did not answer at %s", endpoint)
		}
		retry.Reset(200 * time.Millisecond)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-retry.C:
		}
	}
}
