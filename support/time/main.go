package time

import (
	"strconv"
	goTime "time"
)

//TimeMillis represents time as milliseconds since epoch without any timezone adjustments
type TimeMillis int64

//TimeMillisFromString generates a TimeMillis struct from a string representing an int64
func TimeMillisFromString(s string) (TimeMillis, error) {
	millis, err := strconv.Atoi(s)
	return TimeMillis(int64(millis)), err
}

//FromMillis generates a TimeMillis struct from given millis int64
func FromMillis(millis int64) TimeMillis {
	return TimeMillis(millis)
}

//IsNull returns true if the timeMillis has not been initialized to a date other then 0 from epoch
func (t TimeMillis) IsNull() bool {
	return t == 0
}

//RoundUp returns a new TimeMillis instance with a rounded up to d millis
func (t TimeMillis) RoundUp(d int64) TimeMillis {
	if int64(t)%d != 0 {
		return TimeMillis(int64((int64(t) / d) * (d + 1)))
	}
	return t
}

//RoundUp returns a new TimeMillis instance with a down to d millis
func (t TimeMillis) RoundDown(d int64) TimeMillis {
	//round down to the nearest d
	return TimeMillis(int64(int64(t)/d) * d)
}

//Millis returns the actual int64 millis since epoch
func (t TimeMillis) Millis() int64 {
	return int64(t)
}

//ToDate returns a go time.Time timestamp, UTC adjusted
func (t TimeMillis) ToDate() goTime.Time {
	return goTime.Unix(int64(t)/1000, int64(t)%100*int64(goTime.Millisecond)).UTC()
}
