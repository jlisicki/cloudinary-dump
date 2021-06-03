package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"text/template"
	"time"

	cloudinary "github.com/cloudinary/cloudinary-go"
	"github.com/cloudinary/cloudinary-go/api"
	"github.com/cloudinary/cloudinary-go/api/admin"
	"github.com/dustin/go-humanize"
)

var (
	printHelp          = flag.Bool("h", false, "Print help")
	cloudinaryURL      = flag.String("u", "", "Cloudinary URL with credentials")
	targetFolder       = flag.String("d", "dump", "Target folder for downloaded assets")
	targetNameTemplate = flag.String("t", "{{.PublicID}}", "Template to produce target file name. Uses go template engine with provided struct defined in https://pkg.go.dev/github.com/cloudinary/cloudinary-go@v1.1.0/api/admin#AssetResult")
	concurrency        = flag.Int("c", 5, "How many concurrent connections to use to download assets")
)

type (
	Downloader struct {
		CloudinaryURL                string
		TargetFolder                 string
		TargetFileNameTemplateString string
		Concurrency                  int

		cld                    *cloudinary.Cloudinary
		targetFileNameTemplate *template.Template
	}
	assetList []api.BriefAssetResult
)

func (a assetList) TotalSize() int {
	totalSize := 0
	for i := range a {
		totalSize += a[i].Bytes
	}
	return totalSize
}

func (d *Downloader) collectAllAssets(ctx context.Context) (assetList, error) {
	nextCursor := ""
	result := []api.BriefAssetResult{}
	for {
		page, err := d.cld.Admin.Assets(ctx, admin.AssetsParams{
			NextCursor: nextCursor,
			MaxResults: 1000,
		})
		if err != nil {
			return nil, err
		}
		if page.Error.Message != "" {
			log.Panic(page.Error.Message)
		}
		nextCursor = page.NextCursor
		result = append(result, page.Assets...)
		if page.NextCursor == "" {
			break
		}
	}
	return result, nil
}

func (d *Downloader) init() error {
	var err error
	d.cld, err = cloudinary.NewFromURL(d.CloudinaryURL)
	if err != nil {
		return fmt.Errorf("failed to initialize Cloudinary: %w", err)
	}
	t := template.New("target-file-name")
	d.targetFileNameTemplate, err = t.Parse(d.TargetFileNameTemplateString)
	if err != nil {
		return fmt.Errorf("argument TargetFileNameTemplate has incorrect syntax: %w", err)
	}
	return nil
}

func (d *Downloader) downloadAsset(asset api.BriefAssetResult) error {
	resp, err := http.Get(asset.SecureURL)
	if err != nil {
		return fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()
	var fileName bytes.Buffer
	if err := d.targetFileNameTemplate.Execute(&fileName, asset); err != nil {
		return fmt.Errorf("failed to create file name: %w", err)
	}
	fullFileName := path.Join(d.TargetFolder, fileName.String())
	_, err = os.Stat(fullFileName)
	if !os.IsNotExist(err) {
		return fmt.Errorf("checking file absence failed: %w", err)
	}
	f, err := os.Create(fullFileName)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	_, err = io.Copy(f, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to copy data from network to files: %w", err)
	}
	return nil
}

func (d *Downloader) worker(pending chan api.BriefAssetResult, completed chan api.BriefAssetResult) {
	for asset := range pending {
		err := d.downloadAsset(asset)
		if err != nil {
			log.Fatalf("failed to handle asset %s: %s", asset.AssetID, err)
		}
		completed <- asset
	}
}

// Dump all assets from cloudinary to local folder
func (d *Downloader) Dump(ctx context.Context) error {
	if err := d.init(); err != nil {
		return err
	}
	log.Printf("Collecting information about assets...\n")
	assets, err := d.collectAllAssets(ctx)
	if err != nil {
		return fmt.Errorf("unable to collect information about assets: %w", err)
	}
	log.Printf("Got %d assets with a total size of %s.\n", len(assets), humanize.Bytes(uint64(assets.TotalSize())))
	pending := make(chan api.BriefAssetResult, 1)
	completedAssets := make(chan api.BriefAssetResult, 1)
	finished := make(chan struct{})
	go func(assets assetList, pending chan api.BriefAssetResult) {
		for idx := range assets {
			pending <- assets[idx]
		}
		close(pending)
	}(assets, pending)
	go func(allAssets assetList, completedAssets chan api.BriefAssetResult) {
		completed := assetList{}
		lT := time.Now()
		for asset := range completedAssets {
			completed = append(completed, asset)
			if time.Since(lT) > time.Second {
				log.Printf(
					"Completed %3.1f%%. Downloaded %d/%d files, %s/%s\n",
					float64(len(completed))/float64(len(allAssets))*100,
					len(completed),
					len(allAssets),
					humanize.Bytes(uint64(completed.TotalSize())),
					humanize.Bytes(uint64(allAssets.TotalSize())),
				)
				lT = time.Now()
			}
			if len(completed) == len(allAssets) {
				break
			}
		}
		close(finished)
	}(assets, completedAssets)
	for i := 0; i < d.Concurrency; i++ {
		go d.worker(pending, completedAssets)
	}
	<-finished
	return nil
}

func init() {
	flag.Parse()
}

func main() {
	if *cloudinaryURL == "" || *printHelp {
		flag.PrintDefaults()
		os.Exit(1)
	}
	ctx := context.Background()
	(&Downloader{
		CloudinaryURL:                *cloudinaryURL,
		TargetFolder:                 *targetFolder,
		TargetFileNameTemplateString: *targetNameTemplate,
		Concurrency:                  *concurrency,
	}).Dump(ctx)
}
