package storage

import (
	"fmt"
	"time"
)

// table separation type
type EnumSeparateType int

const (
	SeparateTypeDay   EnumSeparateType = 1 // separate by day
	SeparateTypeMonth EnumSeparateType = 2 // separate by month
	SeparateTypeYear  EnumSeparateType = 3 // separate by year
)

// configuration for table separation
type SeparateTable struct {
	tableName     string           // base table name
	SeparateType  EnumSeparateType // separation type
	LastCheckTime time.Time        // last check time
}

// check if table needs to be separated now
// returns isSeparate (whether separation is needed) and separateTableName (new table name if separated)
func (st *SeparateTable) IsNowSeparate() (isSeparate bool, separateTableName string) {
	switch st.SeparateType {
	case SeparateTypeDay:
		return st.getDayTableName()
	case SeparateTypeMonth:
		return st.getMonthTableName()
	case SeparateTypeYear:
		return st.getYearTableName()
	default:
		return false, "" // invalid separation type
	}
}

// get table name for daily separation
// returns isSeparate (whether separation is needed) and separateTableName (new table name if separated)
func (st *SeparateTable) getDayTableName() (isSeparate bool, separateTableName string) {
	nt := time.Now()
	// check if the last check was on the same day
	if st.LastCheckTime.Year() == nt.Year() && st.LastCheckTime.Month() == nt.Month() && st.LastCheckTime.Day() == nt.Day() {
		return false, "" // no separation needed
	}
	st.LastCheckTime = nt

	// generate table name for the previous day
	nt = nt.AddDate(0, 0, -1)
	return true, fmt.Sprintf("%v_%d%02d%02d", st.tableName, nt.Year(), nt.Month(), nt.Day())
}

// get table name for monthly separation
// returns isSeparate (whether separation is needed) and separateTableName (new table name if separated)
func (st *SeparateTable) getMonthTableName() (isSeparate bool, separateTableName string) {
	nt := time.Now()
	// check if the last check was in the same month
	if st.LastCheckTime.Year() == nt.Year() && st.LastCheckTime.Month() == nt.Month() {
		return false, "" // no separation needed
	}
	st.LastCheckTime = nt

	// generate table name for the previous month
	nt = nt.AddDate(0, -1, 0)
	return true, fmt.Sprintf("%v_%d%02d", st.tableName, nt.Year(), nt.Month())
}

// get table name for yearly separation
// returns isSeparate (whether separation is needed) and separateTableName (new table name if separated)
func (st *SeparateTable) getYearTableName() (isSeparate bool, separateTableName string) {
	nt := time.Now()
	// check if the last check was in the same year
	if st.LastCheckTime.Year() == nt.Year() {
		return false, "" // no separation needed
	}
	st.LastCheckTime = nt

	// generate table name for the previous year
	nt = nt.AddDate(-1, 0, 0)
	return true, fmt.Sprintf("%v_%d", st.tableName, nt.Year())
}
