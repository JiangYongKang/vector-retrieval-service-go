package vector

import "math"

// cosineSimilarity 返回两个等长向量的余弦相似度（范围约 [-1, 1]）。
// 调用方保证 a、b 非空且长度一致；零范数向量返回 0 以避免 NaN（可解释、可复现）。
func cosineSimilarity(a, b []float32) float32 {
	var dot, na, nb float64
	for i := range a {
		x := float64(a[i])
		y := float64(b[i])
		dot += x * y
		na += x * x
		nb += y * y
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return float32(dot / (math.Sqrt(na) * math.Sqrt(nb)))
}
