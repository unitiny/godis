package utils

func ToCmdLine(cmds ...string) [][]byte {
	result := make([][]byte, len(cmds))
	for i, cmd := range cmds {
		result[i] = []byte(cmd)
	}
	return result
}

func ToCmdLine2(action string, cmds [][]byte) [][]byte {
	result := make([][]byte, len(cmds)+1)
	result[0] = []byte(action)
	for i, cmd := range cmds {
		result[i+1] = cmd
	}
	return result
}
