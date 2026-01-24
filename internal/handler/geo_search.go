package handler

import (
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

type geoSearchResult struct {
	member string
	score  float64
	dist   float64
	hash   string
	lat    float64
	long   float64
}

func geosearch(args []string, kv *store.Kv, msgCh chan interface{}) (resp.Value, error) {
	if len(args) < 4 {
		return nil, errors.New("ERR wrong number of arguments for 'geosearch' command")
	}

	key := args[0]
	
	// Parse FROM
	var originLat, originLong float64
	argIdx := 1
	if strings.ToUpper(args[argIdx]) == "FROMMEMBER" {
		if argIdx+1 >= len(args) {
			return nil, errors.New("ERR syntax error")
		}
		member := args[argIdx+1]
		score, ok := kv.ZScore(key, member)
		if !ok {
			return nil, errors.New("ERR member not found")
		}
		originLat, originLong = store.Decode(uint64(score))
		argIdx += 2
	} else if strings.ToUpper(args[argIdx]) == "FROMLONLAT" {
		if argIdx+2 >= len(args) {
			return nil, errors.New("ERR syntax error")
		}
		var err error
		originLong, err = strconv.ParseFloat(args[argIdx+1], 64)
		if err != nil { return nil, errors.New("ERR value is not a valid float") }
		originLat, err = strconv.ParseFloat(args[argIdx+2], 64)
		if err != nil { return nil, errors.New("ERR value is not a valid float") }
		argIdx += 3
	} else {
		return nil, errors.New("ERR syntax error")
	}

	// Parse BY
	var radius float64
	var boxWidth, boxHeight float64
	var unit string
	byBox := false

	if argIdx >= len(args) { return nil, errors.New("ERR syntax error") }

	if strings.ToUpper(args[argIdx]) == "BYRADIUS" {
		if argIdx+2 >= len(args) { return nil, errors.New("ERR syntax error") }
		var err error
		radius, err = strconv.ParseFloat(args[argIdx+1], 64)
		if err != nil { return nil, errors.New("ERR value is not a valid float") }
		unit = args[argIdx+2]
		argIdx += 3
	} else if strings.ToUpper(args[argIdx]) == "BYBOX" {
		if argIdx+3 >= len(args) { return nil, errors.New("ERR syntax error") }
		var err error
		boxWidth, err = strconv.ParseFloat(args[argIdx+1], 64)
		if err != nil { return nil, errors.New("ERR value is not a valid float") }
		boxHeight, err = strconv.ParseFloat(args[argIdx+2], 64)
		if err != nil { return nil, errors.New("ERR value is not a valid float") }
		unit = args[argIdx+3]
		byBox = true
		argIdx += 4
	} else {
		return nil, errors.New("ERR syntax error")
	}

	// Convert radius/box to meters
	toMeters := 1.0
	switch strings.ToLower(unit) {
	case "m": toMeters = 1.0
	case "km": toMeters = 1000.0
	case "ft": toMeters = 0.3048
	case "mi": toMeters = 1609.34
	default: return nil, errors.New("ERR unsupported unit provided. please use m, km, ft, mi")
	}
	radius *= toMeters
	boxWidth *= toMeters
	boxHeight *= toMeters

	// Options
	withCoord := false
	withDist := false
	withHash := false
	count := 0
	asc := true // default ASC

	for argIdx < len(args) {
		opt := strings.ToUpper(args[argIdx])
		switch opt {
		case "WITHCOORD":
			withCoord = true
			argIdx++
		case "WITHDIST":
			withDist = true
			argIdx++
		case "WITHHASH":
			withHash = true
			argIdx++
		case "ASC":
			asc = true
			argIdx++
		case "DESC":
			asc = false
			argIdx++
		case "COUNT":
			if argIdx+1 >= len(args) { return nil, errors.New("ERR syntax error") }
			var err error
			count, err = strconv.Atoi(args[argIdx+1])
			if err != nil { return nil, errors.New("ERR value is not an integer") }
			argIdx += 2
			if argIdx < len(args) && strings.ToUpper(args[argIdx]) == "ANY" {
				argIdx++ // ignore ANY
			}
		default:
			return nil, errors.New("ERR syntax error")
		}
	}

	// Scan all members
	// Optimization: ZRange over full set.
	// We need access to all members.
	// Since we don't have direct access to internal map of KV from handler, we use ZRange(0, -1)
	// But ZRange returns SortedSetNode (member, score).
	// We need to decode score to check distance.
	
	allMembers := kv.ZRange(key, 0, -1)
	if allMembers == nil {
		return resp.NullArray{}, nil // or empty array?
	}

	var results []geoSearchResult

	for _, node := range allMembers {
		lat, long := store.Decode(uint64(node.Score))
		dist := store.Distance(originLat, originLong, lat, long)

		include := false
		if byBox {
			// Simplified Box check (treating lat/long differences as meters locally? No, that's bad for large areas).
			// Redis BYBOX is axis-aligned rectangle.
			// "The command searches for members inside a rectangle... centered at the given position."
			// Wait, is it centered? "centered at the from location"
			// Distance check is simpler. For box, we need relative coordinates?
			// The "width" and "height" are in meters.
			// Implementing precise box search on sphere is complex.
			// Redis docs: "axis-aligned rectangle... The width and height are specified in the units..."
			// We can approximate by checking lat/long diffs converted to meters.
			
			// Approximate conversion:
			// 1 deg lat ~= 111320 meters
			// 1 deg long ~= 111320 * cos(lat) meters
			
			// This is rough but likely what's expected for simple implementation or use Haversine for orthogonal distances?
			// Let's assume the user wants simple inclusion.
			// Let's stick to radius for now if box is too hard, but user asked for GEOSEARCH which implies both.
			// For this challenge, I'll implement Radius correctly. For Box, I'll approximate.
			// But wait, standard GeoSearch logic in Redis uses Geohash coverage.
			
			// Let's implement Box by calculating dLat and dLong in meters.
			// dLatMeters = Distance(lat, long, originLat, long) ? No.
			// dLatMeters = Distance(lat, originLong, originLat, originLong)
			// dLongMeters = Distance(originLat, long, originLat, originLong)
			
			// Check bounds.
			dLatMeters := store.Distance(lat, originLong, originLat, originLong)
			dLongMeters := store.Distance(originLat, long, originLat, originLong)
			
			if dLatMeters <= boxHeight/2 && dLongMeters <= boxWidth/2 {
				include = true
			}

		} else {
			if dist <= radius {
				include = true
			}
		}

		if include {
			results = append(results, geoSearchResult{
				member: node.Member,
				score:  node.Score,
				dist:   dist,
				lat:    lat,
				long:   long,
			})
		}
	}

	// Sort
	sort.Slice(results, func(i, j int) bool {
		if asc {
			return results[i].dist < results[j].dist
		}
		return results[i].dist > results[j].dist
	})

	// Limit
	if count > 0 && len(results) > count {
		results = results[:count]
	}

	// Format Output
	respResults := make(resp.Array, len(results))
	for i, res := range results {
		// If no options, just member string
		if !withCoord && !withDist && !withHash {
			respResults[i] = resp.BulkString(res.member)
		} else {
			// Array of [member, ...]
			row := resp.Array{}
			row = append(row, resp.BulkString(res.member))
			if withDist {
				// Convert back to requested unit?
				// Redis returns distance in the unit specified in BYRADIUS/BYBOX arg.
				d := res.dist / toMeters
				row = append(row, resp.BulkString(strconv.FormatFloat(d, 'f', 4, 64)))
			}
			if withHash {
				row = append(row, resp.Integer(int64(res.score))) // Redis returns integer hash? Or string?
				// "WITHHASH: Also return the hash of the member as an integer (52 bit unsigned integer)."
				row = append(row, resp.Integer(int64(uint64(res.score))))
			}
			if withCoord {
				row = append(row, resp.Array{
					resp.BulkString(strconv.FormatFloat(res.long, 'f', -1, 64)),
					resp.BulkString(strconv.FormatFloat(res.lat, 'f', -1, 64)),
				})
			}
			respResults[i] = row
		}
	}

	return respResults, nil
}
