package util

import "strconv"

func GenerateKey(pk Int64, table string) string {
	key := table + ":" + strconv.FormatInt(pk, 10)
	return key
}
