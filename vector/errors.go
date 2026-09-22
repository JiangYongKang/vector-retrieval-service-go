package vector

import (
	"errors"
	"fmt"
)

// ErrorCode 是可区分的拒绝原因分类。
type ErrorCode string

const (
	CodeInvalidArgument  ErrorCode = "INVALID_ARGUMENT"  // 参数非法（ID 为空、向量维度不一致、TopK 非法等）
	CodeCapacityExceeded ErrorCode = "CAPACITY_EXCEEDED" // 容量不足（记录数 / 批量大小超限）
	CodeTooBusy          ErrorCode = "TOO_BUSY"          // 并发上限触发背压，调用方可重试
	CodeIndexUnavailable ErrorCode = "INDEX_UNAVAILABLE" // 索引暂不可用（服务关闭 / 启动恢复中）
	CodeNotFound         ErrorCode = "NOT_FOUND"         // 记录不存在
)

// ServiceError 携带稳定错误码，便于调用方区分拒绝原因，
// 也便于 HTTP 层映射为不同的状态码与响应体。
type ServiceError struct {
	Code    ErrorCode
	Message string
}

func (e *ServiceError) Error() string {
	return string(e.Code) + ": " + e.Message
}

// newError 创建带稳定错误码的错误，message 支持 fmt 风格格式化。
func newError(code ErrorCode, format string, args ...any) *ServiceError {
	return &ServiceError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// errorCode 提取错误中的稳定错误码；未知错误一律视为索引暂不可用。
func errorCode(err error) ErrorCode {
	if err == nil {
		return ""
	}
	var se *ServiceError
	if errors.As(err, &se) {
		return se.Code
	}
	return CodeIndexUnavailable
}
