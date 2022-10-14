/*
   Copyright The Accelerated Container Image Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package main

import (
	"context"
	"fmt"
	"time"

	obdconv "github.com/containerd/accelerated-container-image/pkg/convertor"
	"github.com/containerd/containerd"
	"github.com/containerd/containerd/cmd/ctr/commands"
	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/platforms"
	_ "github.com/go-sql-driver/mysql"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
)

const (
	labelOverlayBDBlobDigest   = "containerd.io/snapshot/overlaybd/blob-digest"
	labelOverlayBDBlobSize     = "containerd.io/snapshot/overlaybd/blob-size"
	labelOverlayBDBlobFsType   = "containerd.io/snapshot/overlaybd/blob-fs-type"
	labelOverlayBDBlobWritable = "containerd.io/snapshot/overlaybd.writable"
	labelKeyAccelerationLayer  = "containerd.io/snapshot/overlaybd/acceleration-layer"
	labelBuildLayerFrom        = "containerd.io/snapshot/overlaybd/build.layer-from"
	labelDistributionSource    = "containerd.io/distribution.source"
)

var (
	emptyString string
	emptyDesc   ocispec.Descriptor
	emptyLayer  obdconv.Layer

	convSnapshotNameFormat = "overlaybd-conv-%s"
	convLeaseNameFormat    = convSnapshotNameFormat
	convContentNameFormat  = convSnapshotNameFormat
)

type ImageConvertor interface {
	Convert(ctx context.Context, srcManifest ocispec.Manifest, fsType string) (ocispec.Descriptor, error)
}

var convertCommand = cli.Command{
	Name:        "obdconv",
	Usage:       "convert image layer into overlaybd format type",
	ArgsUsage:   "<src-image> <dst-image>",
	Description: `Export images to an OCI tar[.gz] into zfile format`,
	Flags: append(commands.RegistryFlags,
		cli.StringFlag{
			Name:  "fstype",
			Usage: "filesystem type(required), used to mount block device, support specifying mount options and mkfs options, separate fs type and options by ';', separate mount options by ',', separate mkfs options by ' '",
			Value: "ext4",
		},
		cli.StringFlag{
			Name:  "dbstr",
			Usage: "data base config string used for layer deduplication",
			Value: "",
		},
	),
	Action: func(context *cli.Context) error {
		var (
			srcImage    = context.Args().First()
			targetImage = context.Args().Get(1)
		)

		if srcImage == "" || targetImage == "" {
			return errors.New("please provide src image name(must in local) and dest image name")
		}

		cli, ctx, cancel, err := commands.NewClient(context)
		if err != nil {
			return err
		}
		defer cancel()

		ctx, done, err := cli.WithLease(ctx,
			leases.WithID(fmt.Sprintf(convLeaseNameFormat, obdconv.UniquePart())),
			leases.WithExpiration(1*time.Hour),
		)
		if err != nil {
			return errors.Wrap(err, "failed to create lease")
		}
		defer done(ctx)

		var (
			sn = cli.SnapshotService("overlaybd")
			cs = cli.ContentStore()
		)

		fsType := context.String("fstype")
		fmt.Printf("filesystem type: %s\n", fsType)
		dbstr := context.String("dbstr")
		if dbstr != "" {
			fmt.Printf("database config string: %s\n", dbstr)
		}

		srcImg, err := ensureImageExist(ctx, cli, srcImage)
		if err != nil {
			return err
		}

		srcManifest, err := currentPlatformManifest(ctx, cs, srcImg)
		if err != nil {
			return errors.Wrapf(err, "failed to read manifest")
		}

		resolver, err := commands.GetResolver(ctx, context)
		if err != nil {
			return err
		}

		c, err := obdconv.NewOverlaybdConvertor(ctx, cs, sn, resolver, targetImage, dbstr)
		if err != nil {
			return err
		}
		newMfstDesc, err := c.Convert(ctx, srcManifest, fsType)
		if err != nil {
			return err
		}

		newImage := images.Image{
			Name:   targetImage,
			Target: newMfstDesc,
		}
		return createImage(ctx, cli.ImageService(), newImage)
	},
}

func ensureImageExist(ctx context.Context, cli *containerd.Client, imageName string) (containerd.Image, error) {
	return cli.GetImage(ctx, imageName)
}

func currentPlatformManifest(ctx context.Context, cs content.Provider, img containerd.Image) (ocispec.Manifest, error) {
	return images.Manifest(ctx, cs, img.Target(), platforms.Default())
}

func createImage(ctx context.Context, is images.Store, img images.Image) error {
	for {
		if _, err := is.Create(ctx, img); err != nil {
			if !errdefs.IsAlreadyExists(err) {
				return err
			}

			if _, err := is.Update(ctx, img); err != nil {
				if errdefs.IsNotFound(err) {
					continue
				}
				return err
			}
		}
		return nil
	}
}