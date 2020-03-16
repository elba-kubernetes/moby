package daemon // import "github.com/docker/docker/daemon"

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"time"
	"io"
	"strconv"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/backend"
	"github.com/docker/docker/api/types/versions"
	"github.com/docker/docker/api/types/versions/v1p20"
	"github.com/docker/docker/container"
	"github.com/docker/docker/pkg/ioutils"
)

const BUFFER_LENGTH = 512

type BufferedWriter struct {
	Buffer		[BUFFER_LENGTH]byte
	N			int
	Size		int
	Writer		io.Writer
}

func MakeBufferedWriter(dest io.Writer) *BufferedWriter {
	return &BufferedWriter{Writer: dest, Size: BUFFER_LENGTH}
}

func (w *BufferedWriter) Write(p []byte) (int, error) {
	originalLength := len(p)
	srcRemaining := originalLength
	srcStart := 0
	
	// Keep writing and flushing until remaining can fit
	for srcRemaining + w.N > w.Size {
		copy(w.Buffer[w.N:w.Size], p[srcStart:])
		w.Writer.Write(w.Buffer[:])
		
		// Flush
		w.Buffer = [BUFFER_LENGTH]byte{}
		copied := w.Size - w.N
		srcRemaining -= copied
		srcStart += copied
		w.N = 0
	}

	copy(w.Buffer[w.N:], p[srcStart:srcStart+srcRemaining])
	w.N += srcRemaining

	return originalLength, nil
}

func (w *BufferedWriter) Flush() (int, error) {
	originalLength := w.N
	w.Writer.Write(w.Buffer[:w.N])
	w.Buffer = [BUFFER_LENGTH]byte{}
	w.N = 0
	
	return originalLength, nil
}

type LoggerWriter struct {
	Writer	io.Writer
}

func MakeLoggerWriter(dest io.Writer) *LoggerWriter {
	return &LoggerWriter{Writer: dest}
}

func (w *LoggerWriter) Write(p []byte) (int, error) {
	timeNano := strconv.FormatInt(time.Now().UnixNano(), 10)
	timeLen := len(timeNano)
	newBufferLength := timeLen + 1 + len(p) // 1 for space
	buf := make([]byte, newBufferLength)
	copy(buf[:], timeNano[:])
	buf[timeLen] = 32 // space
	copy(buf[timeLen + 1:], p)
	w.Writer.Write(buf)
	
	return newBufferLength, nil
}

// ContainerStats writes information about the container to the stream
// given in the config object.
func (daemon *Daemon) ContainerStats(ctx context.Context, prefixOrName string, config *backend.ContainerStatsConfig) error {
	// Engine API version (used for backwards compatibility)
	apiVersion := config.Version

	container, err := daemon.GetContainer(prefixOrName)
	if err != nil {
		return err
	}

	// If the container is either not running or restarting and requires no stream, return an empty stats.
	if (!container.IsRunning() || container.IsRestarting()) && !config.Stream {
		return json.NewEncoder(config.OutStream).Encode(&types.StatsJSON{
			Name: container.Name,
			ID:   container.ID})
	}

	outStream := config.OutStream
	if config.Stream {
		wf := ioutils.NewWriteFlusher(outStream)
		defer wf.Close()
		wf.Flush()
		outStream = wf
	}

	var preCPUStats types.CPUStats
	var preRead time.Time
	getStatJSON := func(v interface{}) *types.StatsJSON {
		ss := v.(types.StatsJSON)
		ss.Name = container.Name
		ss.ID = container.ID
		ss.PreCPUStats = preCPUStats
		ss.PreRead = preRead
		preCPUStats = ss.CPUStats
		preRead = ss.Read
		return &ss
	}

	// Buffer and add timestamps
	if config.Buffer {
		bufferedWriter := MakeBufferedWriter(outStream)
		defer bufferedWriter.Flush()
		outStream = MakeLoggerWriter(bufferedWriter)
	}

	enc := json.NewEncoder(outStream)
	shouldStream := config.Stream || config.Buffer

	updates := daemon.subscribeToContainerStats(container)
	defer daemon.unsubscribeToContainerStats(container, updates)

	noStreamFirstFrame := true
	for {
		select {
		case v, ok := <-updates:
			if !ok {
				return nil
			}

			var statsJSON interface{}
			statsJSONPost120 := getStatJSON(v)
			if versions.LessThan(apiVersion, "1.21") {
				if runtime.GOOS == "windows" {
					return errors.New("API versions pre v1.21 do not support stats on Windows")
				}
				var (
					rxBytes   uint64
					rxPackets uint64
					rxErrors  uint64
					rxDropped uint64
					txBytes   uint64
					txPackets uint64
					txErrors  uint64
					txDropped uint64
				)
				for _, v := range statsJSONPost120.Networks {
					rxBytes += v.RxBytes
					rxPackets += v.RxPackets
					rxErrors += v.RxErrors
					rxDropped += v.RxDropped
					txBytes += v.TxBytes
					txPackets += v.TxPackets
					txErrors += v.TxErrors
					txDropped += v.TxDropped
				}
				statsJSON = &v1p20.StatsJSON{
					Stats: statsJSONPost120.Stats,
					Network: types.NetworkStats{
						RxBytes:   rxBytes,
						RxPackets: rxPackets,
						RxErrors:  rxErrors,
						RxDropped: rxDropped,
						TxBytes:   txBytes,
						TxPackets: txPackets,
						TxErrors:  txErrors,
						TxDropped: txDropped,
					},
				}
			} else {
				statsJSON = statsJSONPost120
			}

			if !shouldStream && noStreamFirstFrame {
				// prime the cpu stats so they aren't 0 in the final output
				noStreamFirstFrame = false
				continue
			}

			if err := enc.Encode(statsJSON); err != nil {
				return err
			}

			if !shouldStream {
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (daemon *Daemon) subscribeToContainerStats(c *container.Container) chan interface{} {
	return daemon.statsCollector.Collect(c)
}

func (daemon *Daemon) unsubscribeToContainerStats(c *container.Container, ch chan interface{}) {
	daemon.statsCollector.Unsubscribe(c, ch)
}

// GetContainerStats collects all the stats published by a container
func (daemon *Daemon) GetContainerStats(container *container.Container) (*types.StatsJSON, error) {
	stats, err := daemon.stats(container)
	if err != nil {
		return nil, err
	}

	// We already have the network stats on Windows directly from HCS.
	if !container.Config.NetworkDisabled && runtime.GOOS != "windows" {
		if stats.Networks, err = daemon.getNetworkStats(container); err != nil {
			return nil, err
		}
	}

	return stats, nil
}
