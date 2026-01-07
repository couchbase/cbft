//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
	"github.com/couchbase/tools-common/cloud/v8/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v8/objstore/objcli/objaws"
	"github.com/couchbase/tools-common/cloud/v8/objstore/objutil"
	"github.com/couchbase/tools-common/types/iface"
	"github.com/couchbase/tools-common/types/ratelimit"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/time/rate"
)

func GetS3Client(region string) (objcli.Client, error) {
	client := objaws.NewClient(objaws.ClientOptions{ServiceAPI: s3.New(s3.Options{
		Region: region,
	})})
	return client, nil
}

func CheckIfRemotePathIsValid(remotePath string) bool {
	return strings.HasPrefix(remotePath, "s3://")
}

// A sample remote path is of the form:
// pause:s3://<s3-bucket-name>/fts/<path to file>
// This function aims to extract the S3 bucket name and path after the bucket name.
func GetRemoteBucketAndPathHook(remotePath string) (string, string, error) {
	split := strings.SplitN(remotePath, ":", 2)
	if len(split) < 2 {
		return "", "", fmt.Errorf("s3_utils: malformed path")
	}
	cloudURL, err := objutil.ParseCloudOrFileURL(split[1])
	if err != nil {
		return "", "", fmt.Errorf("s3_utils: error parsing remote path: %v", err)
	}

	return cloudURL.Bucket, cloudURL.Path, nil
}

type ProgressReader struct {
	io.Reader
	copyStats *CopyPartitionStats
}

func (pt *ProgressReader) Read(p []byte) (int, error) {
	n, err := pt.Reader.Read(p)
	pt.copyStats.CopyPartitionNumBytesReceived += int32(n)

	return n, err
}

func decompress(src io.Reader, dst string, ctx context.Context, copyStats *CopyPartitionStats) error {
	rateLimitedZr := newRateLimitedReader(ctx, src)

	customReader := &ProgressReader{
		Reader:    rateLimitedZr,
		copyStats: copyStats,
	}

	zr, err := gzip.NewReader(customReader)
	if err != nil {
		return err
	}

	tr := tar.NewReader(zr)

	// uncompress each element
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("s3_utils: context cancelled")
		default:
		}

		header, err := tr.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			atomic.AddInt32(&copyStats.TotCopyPartitionErrors, 1)
			return fmt.Errorf("s3_utils: decompress err: %v", err)
		}

		if header == nil {
			continue
		}
		target := dst + string(os.PathSeparator) + header.Name

		switch header.Typeflag {

		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("s3_utils: error creating directory: %v", err)
			}

		case tar.TypeReg:
			err := os.MkdirAll(filepath.Dir(target), 0766)
			if err != nil {
				return fmt.Errorf("s3_utils: error creating file path %s: %v", target, err)
			}

			fileToWrite, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			// manually close here after each file operation; defering would cause each file close
			// to wait until all operations have completed.
			defer fileToWrite.Close()

			if err != nil {
				return fmt.Errorf("s3_utils: error opening file %s: %v", target, err)
			}

			log.Printf("s3_utils: uncompressing file: %s", target)

			startTime := time.Now()

			_, err = io.Copy(fileToWrite, tr)
			if err != nil {
				return err
			}

			downloadDuration := time.Since(startTime)
			atomic.AddInt32(&copyStats.TotCopyPartitionTimeInMs,
				int32(downloadDuration.Milliseconds()))
		}
	}

	return nil
}

func downloadFromBucket(c objcli.Client, bucket, key, pindexPath string,
	copyStats *CopyPartitionStats, ctx context.Context) error {

	atomic.AddInt32(&copyStats.TotCopyPartitionStart, 1)

	object, err := c.GetObject(ctx, objcli.GetObjectOptions{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		atomic.AddInt32(&copyStats.TotCopyPartitionErrors, 1)
		return err
	}

	resetCopyStats(copyStats, *object.ObjectAttrs.Size)

	// decompressing the tar.gz object and adding it to pindex path
	err = decompress(object.Body, pindexPath, ctx, copyStats)
	if err != nil {
		atomic.AddInt32(&copyStats.TotCopyPartitionErrors, 1)
		return err
	}

	atomic.AddInt32(&copyStats.TotCopyPartitionFinished, 1)
	return nil
}

func DownloadMetadata(client objcli.Client, ctx context.Context, bucket, remotePath string) ([]byte, error) {
	// Ref : https://stackoverflow.com/questions/46019484/buffer-implementing-io-writerat-in-go
	buf := manager.NewWriteAtBuffer([]byte{})

	options := objutil.DownloadOptions{
		Client: client,
		Bucket: bucket,
		Key:    remotePath,
		Writer: newRateLimitedWriterAt(ctx, buf),
	}
	log.Printf("s3_utils: downloading metadata from path %s", remotePath)
	err := objutil.Download(options)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func newRateLimitedReader(ctx context.Context, reader io.Reader) *ratelimit.RateLimitedReader {
	rateLimit := ctx.Value("rateLimit").(uint64)
	// Limit to 'rateLimit' bytes per second.
	rateLimiter := rate.NewLimiter(rate.Every(time.Second/time.Duration(rateLimit)), int(rateLimit))
	return ratelimit.NewRateLimitedReader(ctx, reader, rateLimiter)
}

func newRateLimitedReadAtSeeker(ctx context.Context, reader iface.ReadAtSeeker) *ratelimit.RateLimitedReadAtSeeker {
	rateLimit := ctx.Value("rateLimit").(uint64)
	// Limit to 'rateLimit' bytes per second.
	rateLimiter := rate.NewLimiter(rate.Every(time.Second/time.Duration(rateLimit)), int(rateLimit))
	return ratelimit.NewRateLimitedReadAtSeeker(ctx, reader, rateLimiter)
}

func newRateLimitedWriterAt(ctx context.Context, writer io.WriterAt) *ratelimit.RateLimitedWriterAt {
	rateLimit := ctx.Value("rateLimit").(uint64)
	// Limit to 'rateLimit' bytes per second.
	rateLimiter := rate.NewLimiter(rate.Every(time.Second/time.Duration(rateLimit)), int(rateLimit))
	return ratelimit.NewRateLimitedWriterAt(ctx, writer, rateLimiter)
}

func UploadMetadata(client objcli.Client, ctx context.Context, bucket,
	remotePath string, data []byte) error {
	// Upload a file without saving it - S3
	// Ref: https://stackoverflow.com/questions/47621804/upload-object-to-aws-s3-without-creating-a-file-using-aws-sdk-go
	reader := strings.NewReader(string(data))

	options := objutil.UploadOptions{
		Client:  client,
		Bucket:  bucket,
		Key:     remotePath,
		Body:    newRateLimitedReadAtSeeker(ctx, reader),
		Options: objutil.Options{Context: ctx},
	}
	log.Printf("s3_utils: uploading metadata to path %s", remotePath)
	_, err := objutil.Upload(options)
	if err != nil {
		log.Errorf("s3_utils: error uploading index defs: %v", err)
	}
	return err
}

// Uploads the pindex directory to S3 for a hibernated index.
func uploadPIndexFiles(mgr *cbgt.Manager, client objcli.Client, remotePath,
	pindexName, path string, ctx context.Context) {
	_, pindexes := mgr.CurrentMaps()
	pindex, exists := pindexes[pindexName]
	if !exists {
		log.Errorf("s3_utils: pindex %s not in mgr cache", pindexName)
		return
	}

	destForwarder, ok := pindex.Dest.(*cbgt.DestForwarder)
	if !ok {
		log.Errorf("s3_utils: unable to find dest forwarder and upload pindex")
		return
	}
	dest, ok := destForwarder.DestProvider.(*BleveDest)
	if !ok {
		log.Errorf("s3_utils: unable to find dest and upload pindex")
		return
	}

	bucket, keyPrefix, err := GetRemoteBucketAndPathHook(remotePath)
	if err != nil {
		log.Errorf("s3_utils: error getting bucket and key from remote path: %v", err)
		atomic.AddInt32(&dest.copyStats.TotCopyPartitionErrors, 1)
		return
	}

	atomic.AddInt32(&dest.copyStats.TotCopyPartitionStart, 1)

	log.Printf("s3_utils: uploading path %s", path)
	err = compressUploadToBucket(client, bucket, path, keyPrefix, dest.copyStats, ctx)
	if err != nil {
		log.Errorf("s3_utils: error compressing and uploading to bucket: %v", err)
		atomic.AddInt32(&dest.copyStats.TotCopyPartitionErrors, 1)
		return
	}

	atomic.AddInt32(&dest.copyStats.TotCopyPartitionFinished, 1)
}

func compressUploadToBucket(c objcli.Client, bucket, pindexPath,
	keyPrefix string, copyStats *CopyPartitionStats, ctx context.Context) error {
	pindexName := cbgt.PIndexNameFromPath(pindexPath)
	key := keyPrefix + "/" + pindexName + ".tar.gz"

	mpuOpts := objutil.MPUploaderOptions{
		Client:  c,
		Key:     key,
		Bucket:  bucket,
		Options: objutil.Options{Context: ctx},
	}
	mpUploader, err := objutil.NewMPUploader(mpuOpts)
	if err != nil {
		return fmt.Errorf("s3_utils: failed to create uploader: %v", err)
	}

	startTime := time.Now()

	errs := compressUploadUtil(pindexPath, ctx, mpUploader)
	if errs != nil && len(errs) > 0 {
		atomic.AddInt32(&copyStats.TotCopyPartitionErrors, int32(len(errs)))
		log.Errorf("s3_utils: errors compressing path %s: ", pindexPath)
		for _, err := range errs {
			log.Errorf("%v", err)
		}
	}

	atomic.AddInt32(&copyStats.TotCopyPartitionTimeInMs, int32(time.Since(startTime).Milliseconds()))

	return nil
}

// Resets the copy partition stats of the pindex to the expected
// values before transfer(upload/download).
func resetCopyStats(copyStats *CopyPartitionStats, totalObjSize int64) {
	atomic.StoreInt32(&copyStats.CopyPartitionNumBytesExpected, int32(totalObjSize))
	// Resetting it to 0 prior to each pindex transfer.
	atomic.StoreInt32(&copyStats.CopyPartitionNumBytesReceived, 0)
	atomic.StoreInt32(&copyStats.TotCopyPartitionErrors, 0)
}

func compressUploadUtil(src string, ctx context.Context, mpUploader *objutil.MPUploader) []error {
	// A pipe reader and writer are used to synchronously read compressed
	// bytes to be uploaded as they are being generated, without needing to
	// wait for compression to be completed for all the files.
	r, w := io.Pipe()
	zw := gzip.NewWriter(w)
	tw := tar.NewWriter(zw)

	var wg sync.WaitGroup
	wg.Add(2)

	var errMutex sync.RWMutex
	var errs []error
	appendToErrs := func(err error) {
		errMutex.Lock()
		errs = append(errs, err)
		errMutex.Unlock()
	}

	// This goroutine reads bytes from the writer and performs a multipart upload to
	// object storage, till it hits EOF.
	go func() {
		defer func() {
			r.Close()
			wg.Done()
		}()

		// 6 MB in bytes
		uploadSliceSize := 6 * 1024 * 1024

		for {
			slice := make([]byte, uploadSliceSize)
			pos, readErr := io.ReadFull(r, slice)
			if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
				appendToErrs(fmt.Errorf("s3_utils: could not read: %v", readErr))
				return
			}

			buf := bytes.NewReader(slice[:pos])
			err := mpUploader.Upload(newRateLimitedReadAtSeeker(ctx, buf))
			if err != nil {
				appendToErrs(fmt.Errorf("s3_utils: error MP upload: %v", err))
				return
			}

			// Have hit an EOF here if we didn't return readErr earlier.
			if readErr != nil {
				break
			}
		}

		_, err := mpUploader.Commit()
		if err != nil {
			appendToErrs(err)
		}
	}()

	// This goroutine walks through the pindex files and writes the compressed bytes
	// into the tar writer, which is then received by the reader.
	go func() {
		defer func() {
			if err := tw.Close(); err != nil {
				appendToErrs(fmt.Errorf("s3_utils: error closing tar writer: %v", err))
			}

			if err := zw.Close(); err != nil {
				appendToErrs(fmt.Errorf("s3_utils: error closing zip writer: %v", err))
			}

			if err := w.Close(); err != nil {
				appendToErrs(fmt.Errorf("s3_utils: error closing pipe writer: %v", err))
			}
			wg.Done()
		}()

		// walk through every file in the pindex folder
		err := filepath.Walk(src, func(file string, fi os.FileInfo, err error) error {
			if isCanceled(ctx) {
				return ctx.Err()
			}

			// Don't upload pindex_meta file
			if strings.HasSuffix(file, cbgt.PINDEX_META_FILENAME) {
				return nil
			}

			// generate tar header
			header, err := tar.FileInfoHeader(fi, file)
			if err != nil {
				return err
			}

			// Upload files without the pindex path.
			if !fi.IsDir() {
				header.Name = file[len(src)+1:]
			} else {
				header.Name = filepath.ToSlash(file)
			}

			// if not a dir, write file content
			if !fi.IsDir() {
				log.Printf("s3_utils: compressing file %s", file)

				// write header
				if err := tw.WriteHeader(header); err != nil {
					return err
				}

				data, err := os.Open(file)
				if err != nil {
					return err
				}
				_, err = io.Copy(tw, data)
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			appendToErrs(err)
		}
	}()

	wg.Wait()

	return errs
}
