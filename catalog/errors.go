package catalog

import (
	"fmt"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

type SubdirectoryDoesNotContainFiles string

func (msg SubdirectoryDoesNotContainFiles) Error() string {
	return errReport("%s: Subdirectory in catalog does not contain files", string(msg))
}

type PathNotContainedInExistingCategory string

func (msg PathNotContainedInExistingCategory) Error() string {
	return errReport("%s: Path does not reference a subdirectory in this catalog", string(msg))
}

type UnableToCreateFile string

func (msg UnableToCreateFile) Error() string {
	return errReport("%s: Unable to create file", string(msg))
}

type UnableToLocateSubDir string

func (msg UnableToLocateSubDir) Error() string {
	return errReport("%s: Unable to find subdirectory in catalog", string(msg))
}

type UnableToWriteHeader string

func (msg UnableToWriteHeader) Error() string {
	return errReport("%s: Unable to write header for new file", string(msg))
}

type FileAlreadyExists string

func (msg FileAlreadyExists) Error() string {
	return errReport("%s: File is already in directory", string(msg))
}

type NotFoundError string

func (msg NotFoundError) Error() string {
	return errReport("%s: Path not found", string(msg))
}

func errReport(base string, msg string) string {
	base = io.GetCallerFileContext(2) + ":" + base
	return fmt.Sprintf(base, msg)
}

// ErrCategoryFileNotFound is used when "category_name" file under each data directory is not found.
type ErrCategoryFileNotFound struct {
	filePath string
	msg string
}

func (e *ErrCategoryFileNotFound) Error() string {
	return "Could not find a category_name file under:" + e.filePath +", msg="+ e.msg
}
