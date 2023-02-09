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
	flagLocalDir   = flag.String("l", "", "local")
	flagRemoteDir  = flag.String("r", "", "remote")
	flagAlias      = flag.String("a", "", "alias")
	flagConfigPath = flag.String("c", "", "config")
)

func main() {
	flag.Parse()
	startTime := time.Now()
	logsParam := "[test]"
	defer func(time1 time.Time) {
		fmt.Println(fmt.Sprintf("%s end-cost Seconds:%v\n", logsParam, time.Since(time1)))
	}(startTime)
	aliasCfg := loadRcloneConfig()[*flagAlias]
	copyDirToRemote(aliasCfg, flagLocalDir, flagRemoteDir)
}

func copyDirToRemote(aliasCfg interface{}, ld, rd *string) {
	if ld == nil || rd == nil {
		panic("local or remote dir is nil")
	}
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
	err := config.SetConfigPath(*flagConfigPath)
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
