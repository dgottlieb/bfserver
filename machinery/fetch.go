package machinery

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func Untar(tarball, target string) error {
	reader, err := os.Open(tarball)
	if err != nil {
		return err
	}
	defer reader.Close()
	tarReader := tar.NewReader(reader)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		path := filepath.Join(target, header.Name)
		info := header.FileInfo()
		if info.IsDir() {
			if err = os.MkdirAll(path, info.Mode()); err != nil {
				return err
			}
			continue
		}

		file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
		if err != nil {
			return err
		}
		defer file.Close()
		_, err = io.Copy(file, tarReader)
		if err != nil {
			return err
		}
	}
	return nil
}

func UnGzip(source, target string) error {
	reader, err := os.Open(source)
	if err != nil {
		return err
	}
	defer reader.Close()

	archive, err := gzip.NewReader(reader)
	if err != nil {
		return err
	}
	defer archive.Close()

	target = filepath.Join(target, archive.Name)
	writer, err := os.Create(target)
	if err != nil {
		return err
	}
	defer writer.Close()

	_, err = io.Copy(writer, archive)
	return err
}

func FetchArtifactsForTask(task string, target string) []string {
	if !strings.HasSuffix(target, "/") {
		panic("needs trailing /")
	}

	if err := os.RemoveAll(target); err != nil {
		panic(err)
	}

	if err := os.MkdirAll(target+"dbpath", 0755); err != nil {
		panic(err)
	}

	evg := exec.Command("evergreen", "fetch", "--task", task, "--artifacts", "--shallow", "--dir", target)
	if err := evg.Run(); err != nil {
		panic(err)
	}

	dir := os.DirFS(target)
	matches, err := fs.Glob(dir, "artifacts-*/mongo-data-*")
	if err != nil {
		panic(err)
	}

	dataTgz, err := os.Open(target + matches[0])
	if err != nil {
		panic(err)
	}

	gzReader, err := gzip.NewReader(dataTgz)
	if err != nil {
		panic(err)
	}

	tarReader := tar.NewReader(gzReader)
	for {
		header, tarErr := tarReader.Next()
		if tarErr == io.EOF {
			break
		}

		path := filepath.Join(target+"dbpath", header.Name)
		info := header.FileInfo()
		if info.IsDir() {
			if err = os.MkdirAll(path, 0755); err != nil {
				panic(err)
			}
			continue
		}

		out, err := os.Create(path)
		if err != nil {
			panic(err)
		}

		written, err := io.Copy(out, tarReader)
		if err != nil {
			panic(err)
		}
		if written != header.Size {
			panic("bad size")
		}
	}

	dbpaths := make([]string, 0)
	fs.WalkDir(os.DirFS(target+"dbpath/"), ".", func(path string, dir fs.DirEntry, err error) error {
		if err != nil {
			panic(err)
		}

		if dir.Name() == "WiredTiger" {
			dbpaths = append(dbpaths, target+"dbpath/"+filepath.Dir(path))
		}
		return nil
	})

	return dbpaths
}
