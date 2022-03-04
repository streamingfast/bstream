package transform

import (
	"fmt"
	"strconv"
	"strings"
)

// lowBoundary helps to determine the offset of a given integer value
func lowBoundary(i uint64, mod uint64) uint64 {
	return i - (i % mod)
}

// toIndexFilename concatenates an index filename
func toIndexFilename(bundleSize, baseBlockNum uint64, shortname string) string {
	return fmt.Sprintf("%010d.%d.%s.idx", baseBlockNum, bundleSize, shortname)
}

func parseIndexFilename(name string) (bundleSize, baseBlockNum uint64, shortname string, err error) {
	parts := strings.Split(name, ".")
	if len(parts) != 4 || parts[3] != "idx" {
		err = fmt.Errorf("invalid parsed index filename: %s", name)
		return
	}
	baseBlockNum, err = strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return
	}

	bundleSize, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return
	}

	shortname = parts[2]
	return
}
