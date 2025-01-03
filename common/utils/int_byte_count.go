package utils

func CountIntByte(i int64) int {
	ret := 0
	if i < 0 {
		i = -i
		ret++
	}
	for i > 1e5 {
		ret += 5
		i /= 1e5
	}
	for {
		ret++
		i = i / 10
		if i == 0 {
			return ret
		}
	}
}
