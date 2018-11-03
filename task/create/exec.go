// Package create is the package of create and make Ken happy.
package create

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"overlord/lib/dir"
	"overlord/lib/etcd"
	"overlord/lib/log"
	"path/filepath"

	"overlord/config"

	"overlord/proto"

	"overlord/lib/systemd"

	"github.com/BurntSushi/toml"
	"text/template"
)

func getDefaultServiceWorkDir() string {
	if config.GetRunMode() != config.RunModeTest {
		return "/data/%d"
	}
	return "/tmp/data/%d"
}

// DeployInfo is the struct to communicate between etcd and executor
// must be serialized and deserialized by json
type DeployInfo struct {
	// TaskID is the id of task
	TaskID string

	CacheType proto.CacheType

	Port    int
	Version string

	// TplTree is the Tree which contains a key as path of the file,
	// and value as the content of the file.
	TplTree    map[string]string
	FileServer string
}

// GenDeployInfo will create new deploy info from etcd
func GenDeployInfo(e *etcd.Etcd, ip string, port int) (info *DeployInfo, err error) {
	var (
		val         string
		instanceDir = fmt.Sprintf(InstancePath, ip, port)
		workdir     = fmt.Sprintf(getDefaultServiceWorkDir(), port)
	)

	info.TplTree = make(map[string]string)
	val, err = e.Get(context.TODO(), fmt.Sprintf("%s/CacheType", instanceDir))
	if err != nil {
		return
	}
	info.CacheType = proto.CacheType(val)

	if info.CacheType == proto.CacheTypeRedisCluster {
		val, err = e.Get(context.TODO(), fmt.Sprintf("%s/redis.conf", instanceDir))

		if err != nil {
			return
		}
		info.TplTree[fmt.Sprintf("%s/redis.conf", workdir)] = val

		val, err = e.Get(context.TODO(), fmt.Sprintf("%s/nodes.conf", instanceDir))
		if err != nil {
			return
		}
		info.TplTree[fmt.Sprintf("%s/nodes.conf", workdir)] = val
	} else if info.CacheType == proto.CacheTypeRedis {
		val, err = e.Get(context.TODO(), fmt.Sprintf("%s/redis.conf", instanceDir))
		if err != nil {
			return
		}
		info.TplTree[fmt.Sprintf("%s/redis.conf", workdir)] = val
	} else if info.CacheType == proto.CacheTypeMemcache {
		val, err = e.Get(context.TODO(), fmt.Sprintf("%s/memcached.conf", instanceDir))
		if err != nil {
			return
		}
		info.TplTree[fmt.Sprintf("%s/memcached.conf", workdir)] = val
	} else {
		log.Errorf("unsupported cachetype %s", info.CacheType)
	}

	info.FileServer, err = e.Get(context.TODO(), "/fileserver")
	if err != nil {
		return
	}
	info.TaskID, err = e.Get(context.TODO(), fmt.Sprintf("%s/taskid", instanceDir))
	if err != nil {
		return
	}

	info.Version, err = e.Get(context.TODO(), fmt.Sprintf("%s/version", instanceDir))
	return
}

func renderTplTree(tplTree map[string]string) (err error) {
	var (
		basename string
		abs      string
	)

	for path, content := range tplTree {
		basename, err = dir.GetAbsDir(path)
		if err != nil {
			return
		}
		err = dir.MkDirAll(basename)
		if err != nil {
			return
		}
		abs, err = filepath.Abs(path)
		if err != nil {
			return
		}

		err = ioutil.WriteFile(abs, []byte(content), 0755)
		if err != nil {
			return
		}
	}

	return
}

func renderMetaIntoFile(workdir string, di *DeployInfo) error {
	file := fmt.Sprintf("%s/meta.toml", workdir)
	fd, err := os.Create(file)
	if err != nil {
		return err
	}
	err = fd.Chmod(0755)
	if err != nil {
		return err
	}
	encoder := toml.NewEncoder(fd)

	return encoder.Encode(di)
}

func checkBinaryVersion(cacheType proto.CacheType, version string) bool {
	var path string
	if cacheType == proto.CacheTypeMemcache {
		path = fmt.Sprintf("/data/lib/memcache/%s/bin/memcached", version)
	} else {
		path = fmt.Sprintf("/data/lib/redis/%s/bin/redis-server", version)
	}

	exits, err := dir.IsExists(path)
	if err != nil {
		log.Warnf("check exists fail due to %s", err)
		return false
	}
	return exits
}

func downloadFile(filepath string, url string) (err error) {

	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Check server response
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	// Writer the body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}
	return nil
}

func downloadBinary(info *DeployInfo) error {
	var cacheType string
	if info.CacheType == proto.CacheTypeMemcache {
		cacheType = "memcache"
	} else {
		cacheType = "redis"
	}
	url := fmt.Sprintf("%s/%s/%s.tar.gz", info.FileServer, cacheType, info.Version)

	fileName := fmt.Sprintf("/tmp/overlord/%s-%s.tar.gz", cacheType, info.Version)
	err := downloadFile(fileName, url)
	if err != nil {
		return err
	}

	baseDir, err := dir.GetAbsDir(fileName)
	if err != nil {
		return err
	}
	fd, err := os.Open(fileName)
	if err != nil {
		return err
	}
	tmp := fmt.Sprintf("%s/%s-%s", baseDir, cacheType, info.Version)
	err = extractTarGz(tmp, fd)
	if err != nil {
		return err
	}

	err = dir.MkDirAll(fmt.Sprintf("/data/lib/%s", cacheType))
	if err != nil {
		return err
	}
	targetDir := fmt.Sprintf("/data/lib/%s/%s/", cacheType, info.Version)

	return os.Rename(tmp, targetDir)
}

func extractTarGz(baseDir string, gzipStream io.Reader) error {
	uncompressedStream, err := gzip.NewReader(gzipStream)
	if err != nil {
		return err
	}
	tarReader := tar.NewReader(uncompressedStream)
	err = dir.MkDirAll(baseDir)
	if err != nil {
		return err
	}

	for true {
		header, err := tarReader.Next()

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Errorf("ExtractTarGz: Next() failed: %s", err.Error())
			return err
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.Mkdir(fmt.Sprintf("%s/%s", baseDir, header.Name), 0755); err != nil {
				log.Errorf("ExtractTarGz: Mkdir() failed: %s", err.Error())
				return err
			}
		case tar.TypeReg:
			outFile, err := os.Create(fmt.Sprintf("%s/%s", baseDir, header.Name))
			if err != nil {
				log.Errorf("ExtractTarGz: Create() failed: %s", err.Error())
				return err
			}
			defer outFile.Close()
			if _, err := io.Copy(outFile, tarReader); err != nil {
				log.Errorf("ExtractTarGz: Copy() failed: %s", err.Error())
				return err
			}
		default:
			log.Errorf(
				"ExtractTarGz: uknown type: %s in %s",
				header.Typeflag,
				header.Name)
			return errors.New("uknown compression type")
		}
	}
	return nil
}

func buildServiceName(cacheType proto.CacheType, port int) string {
	if cacheType == proto.CacheTypeMemcache {
		return fmt.Sprintf("memcache@%d.service", port)
	}
	return fmt.Sprintf("redis@%d.service", port)
}

func setupSystemdServiceFile(info *DeployInfo) error {
	var (
		fname string
		tplBody string
	)

	if info.CacheType == proto.CacheTypeRedis || info.CacheType == proto.CacheTypeRedisCluster {
		fname = fmt.Sprintf("/etc/systemd/system/redis-%s@.service", info.Version)
		tplBody = config.RedisServiceTpl
	} else if info.CacheType == proto.CacheTypeMemcache {
		fname = fmt.Sprintf("/etc/systemd/system/memcache-%s@.service", info.Version)
		tplBody = config.MemcacheServiceTpl
	}

	fd, err := os.Create(fname)
	if err != nil {
		return err
	}
	tpl, err := template.New("service").Parse(tplBody)
	if err != nil {
		return err
	}
	return tpl.Execute(fd, map[string]string{"Version": info.Version})
}

// SetupCacheService will create new cache service
func SetupCacheService(info *DeployInfo) error {

	// 1. render template tree into the path
	//   1.1 foreach fpath, content in TplTree
	//   1.4 mkdir for fpath's basedir
	//   1.3 write content into
	err := renderTplTree(info.TplTree)
	if err != nil {
		log.Warnf("error when render template tree")
		return err
	}

	// 2. execute given command
	//   2.0 mk working dir
	workdir := fmt.Sprintf(getDefaultServiceWorkDir(), info.Port)
	err = dir.MkDirAll(workdir)
	if err != nil {
		log.Errorf("fail to create working dir")
		return err
	}
	err = renderMetaIntoFile(workdir, info)
	if err != nil {
		log.Errorf("fail to create meta data file due to %s", err)
		return err
	}

	// 2. setup systemd serivce
	//   2.1 check if binary was exists
	exists := checkBinaryVersion(info.CacheType, info.Version)
	if !exists {
		//   2.2 if not, pull it from scheduler and then setup systemd config
		if err = downloadBinary(info); err != nil {
			return err
		}
		if err = setupSystemdServiceFile(info); err != nil {
			return err
		}
	}
	// 3. spawn a new redis cluster service
	serviceName := buildServiceName(info.CacheType, info.Port)
	return systemd.Start(serviceName)
}
