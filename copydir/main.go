package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/FlyTOmeLight/normaltest/filesystem"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/rclone/rclone/fs/rc"
)

var (
	flagLocalDir  = flag.String("l", "", "local")
	flagRemoteDir = flag.String("r", "", "remote")
	alias         = "devmodel02-8077"
)

func main() {
	flag.Parse()
	aliasCfg := loadRcloneConfig()[alias]
	copyDirToRemote(aliasCfg, flagLocalDir, flagRemoteDir)
}

// @brief：耗时统计函数
func timeCost() func() {
	start := time.Now()
	return func() {
		tc := time.Since(start)
		fmt.Printf("time cost = %v\n", tc)
	}
}

func copyDirToRemote(aliasCfg interface{}, ld, rd *string) {
	if ld == nil || rd == nil {
		panic("local or remote dir is nil")
	}
	defer timeCost()
	lbs, err := filesystem.NewBlobStore(filesystem.KindLocal, "/", nil)
	if err != nil {
		panic(err)
	}
	rbs, err := filesystem.NewBlobStore(filesystem.KindS3, "benchmark", map[string]string{
		"ak":         aliasCfg.(rc.Params)["access_key_id"].(string),
		"sk":         aliasCfg.(rc.Params)["secret_access_key"].(string),
		"host":       aliasCfg.(rc.Params)["endpoint"].(string),
		"region":     aliasCfg.(rc.Params)["region"].(string),
		"disableSSL": "true",
	})
	if err != nil {
		panic(err)
	}
	err = filepath.Walk(*ld, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		locRelativePath := strings.TrimPrefix(path, *ld)
		realS3Path := fmt.Sprintf("%s%s", *rd, locRelativePath)
		relativePath := strings.TrimPrefix(path, "/")
		err = filesystem.CopyRaw(lbs, rbs, relativePath, realS3Path)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}

func loadRcloneConfig() rc.Params {
	err := config.SetConfigPath("/Users/jojo/.config/rclone/rclone.conf")
	if err != nil {
		panic(err)
	}
	err = config.Data().Load()
	if err != nil {
		panic(err)
	}
	configfile.Install()
	accounting.Start(context.Background())
	return config.DumpRcBlob()
}
