package storage

import (
	"fmt"
	"time"
)

// 分表类型
type EnumSeparateType int

const (
	SeparateTypeDay   EnumSeparateType = 1 // 按天
	SeparateTypeMonth EnumSeparateType = 2 // 按月
	SeparateTypeYear  EnumSeparateType = 3 // 按年
)

// 分表存储配置
type SeparateTable struct {
	tableName     string           // 表名
	SeparateType  EnumSeparateType // 分表类型
	LastCheckTime time.Time        // 上次检测时间
}

// 当前是否需分表,isSeparate=是否需要分表, separateTableName=分表名称
func (this *SeparateTable) IsNowSeparate() (isSeparate bool, separateTableName string) {
	switch this.SeparateType {
	case SeparateTypeDay:
		return this.getDayTableName()
	case SeparateTypeMonth:
		return this.getMonthTableName()
	case SeparateTypeYear:
		return this.getYearTableName()
	default:
		return false, ""
	}
}

// 按日分表
func (this *SeparateTable) getDayTableName() (isSeparate bool, separateTableName string) {
	nt := time.Now()
	if this.LastCheckTime.Year() == nt.Year() && this.LastCheckTime.Month() == nt.Month() && this.LastCheckTime.Day() == nt.Day() {
		return false, ""
	}
	this.LastCheckTime = nt

	nt = nt.AddDate(0, 0, -1)
	return true, fmt.Sprintf("%v_%d%02d%02d", this.tableName, nt.Year(), nt.Month(), nt.Day())
}

// 按月分表
func (this *SeparateTable) getMonthTableName() (isSeparate bool, separateTableName string) {
	nt := time.Now()
	if this.LastCheckTime.Year() == nt.Year() && this.LastCheckTime.Month() == nt.Month() {
		return false, ""
	}
	this.LastCheckTime = nt

	nt = nt.AddDate(0, -1, 0)
	return true, fmt.Sprintf("%v_%d%02d", this.tableName, nt.Year(), nt.Month())
}

// 按年分表
func (this *SeparateTable) getYearTableName() (isSeparate bool, separateTableName string) {
	nt := time.Now()
	if this.LastCheckTime.Year() == nt.Year() {
		return false, ""
	}
	this.LastCheckTime = nt

	nt = nt.AddDate(-1, 0, 0)
	return true, fmt.Sprintf("%v_%d", this.tableName, nt.Year())
}
