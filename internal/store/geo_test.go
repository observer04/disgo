package store

import (
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	lat, long := 37.7749, -122.4194 // San Francisco
	hash := Encode(lat, long)
	dLat, dLong := Decode(hash)

	if abs(dLat-lat) > 0.0001 {
		t.Errorf("Lat mismatch: got %f, want %f", dLat, lat)
	}
	if abs(dLong-long) > 0.0001 {
		t.Errorf("Long mismatch: got %f, want %f", dLong, long)
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
