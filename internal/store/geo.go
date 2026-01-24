package store

import (
	"math"
)

// Constants for WGS84 Earth radius
const EarthRadiusMeters = 6372797.560856

// Mercator projection limits;
const (
	MercatorMax = 20037726.37
	MercatorMin = -20037726.37
)

// Limits from Redis implementation
var (
	LatRange  = [2]float64{-85.05112878, 85.05112878}
	LongRange = [2]float64{-180, 180}
)

// Encode encodes a latitude and longitude into a 52-bit integer geohash
// This uses the standard interleave algorithm.
func Encode(lat, long float64) uint64 {
	latMin, latMax := LatRange[0], LatRange[1]
	longMin, longMax := LongRange[0], LongRange[1]

	if lat < latMin || lat > latMax || long < longMin || long > longMax {
		return 0 // Error/Out of bounds
	}

	var hash uint64
	step := 26 // 52 bits total, 26 for each

	for i := 0; i < step; i++ {
		hash <<= 1
		// Bisect Longitude
		mid := (longMin + longMax) / 2
		if long > mid {
			hash |= 1
			longMin = mid
		} else {
			longMax = mid
		}

		hash <<= 1
		// Bisect Latitude
		mid = (latMin + latMax) / 2
		if lat > mid {
			hash |= 1
			latMin = mid
		} else {
			latMax = mid
		}
	}
	return hash
}

// Decode decodes a 52-bit integer geohash into latitude and longitude
func Decode(hash uint64) (float64, float64) {
	latMin, latMax := LatRange[0], LatRange[1]
	longMin, longMax := LongRange[0], LongRange[1]

	for i := 0; i < 26; i++ {
		// Bit index for long: 51 - 2*i
		// Bit index for lat:  50 - 2*i

		// Longitude bit
		if (hash>>(51-2*i))&1 == 1 {
			longMin = (longMin + longMax) / 2
		} else {
			longMax = (longMin + longMax) / 2
		}

		// Latitude bit
		if (hash>>(50-2*i))&1 == 1 {
			latMin = (latMin + latMax) / 2
		} else {
			latMax = (latMin + latMax) / 2
		}
	}

	return (latMin + latMax) / 2, (longMin + longMax) / 2
}

// Distance calculates the Haversine distance between two points in meters
func Distance(lat1, lon1, lat2, lon2 float64) float64 {
	toRad := math.Pi / 180
	lat1 *= toRad
	lon1 *= toRad
	lat2 *= toRad
	lon2 *= toRad

	dLat := lat2 - lat1
	dLon := lon2 - lon1

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*
			math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return EarthRadiusMeters * c
}

// Base32 Alphabet for Geohash
var base32Alphabet = "0123456789bcdefghjkmnpqrstuvwxyz"

// ToGeohashString converts a 52-bit integer geohash to a standard 11-character geohash string
func ToGeohashString(hash uint64) string {
	// Standard geohash string is usually based on 5-bit chunks.
	// 11 chars * 5 bits = 55 bits.
	// We have 52 bits.
	// Redis implementation details might differ slightly but let's try standard approach.
	// Actually Redis uses 52 bits which maps to about 10-11 chars.

	// Let's implement the standard string construction.
	// We need to re-encode using 5-bit precision.
	// Or we can assume 'hash' is already the interleaved bits.

	// Wait, the standard textual geohash doesn't map 1:1 to the 52-bit integer perfectly aligned?
	// The 52-bit integer is used for ZSET scoring (sorting).
	// The string representation is just another encoding of the lat/long.
	// So better to re-encode from lat/long OR unpack the hash.

	// Unpacking hash is effectively:
	// Take 5 bits at a time from the interleaved stream?
	// But our hash was built 2-bits at a time (interleaved).

	// Let's decode to lat/long first, then encode to string?
	// That's safer.

	lat, long := Decode(hash)
	return EncodeToString(lat, long)
}

func EncodeToString(lat, long float64) string {
	chars := make([]byte, 11)
	latMin, latMax := LatRange[0], LatRange[1]
	longMin, longMax := LongRange[0], LongRange[1]

	bit := 0
	ch := 0
	idx := 0

	even := true
	// even bit: longitude, odd bit: latitude
	for idx < 11 {
		ch <<= 1
		if even {
			// Longitude
			mid := (longMin + longMax) / 2
			if long > mid {
				ch |= 1
				longMin = mid
			} else {
				longMax = mid
			}
		} else {
			// Latitude
			mid := (latMin + latMax) / 2
			if lat > mid {
				ch |= 1
				latMin = mid
			} else {
				latMax = mid
			}
		}

		even = !even
		bit++
		// Every 5 bits, we have a character
		if bit == 5 {
			chars[idx] = base32Alphabet[ch] // Map 0-31 to char
			idx++
			bit = 0
			ch = 0
		}
	}
	return string(chars)
}
