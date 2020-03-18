package daemon // import "github.com/docker/docker/daemon"

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/backend"
	"github.com/docker/docker/api/types/versions"
	"github.com/docker/docker/api/types/versions/v1p20"
	"github.com/docker/docker/container"
	"github.com/docker/docker/pkg/ioutils"
	"github.com/sirupsen/logrus"
)

// Lines are ~1200 chars long, so this is ~14 lines at once
const BUFFER_LENGTH = 16384

func (daemon *Daemon) ShouldCollectStats(container *container.Container) bool {
	// TODO investigate discrimination schemas for excluding kube-proxy/kubelet
	return true
}

func (daemon *Daemon) StopStatsLogs(container *container.Container) {
	daemon.statsLoggers.RLock()
	if cancel, ok := daemon.statsLoggers.m[container.ID]; ok {
		cancel()
		daemon.statsLoggers.RUnlock()

		logrus.Debugf("Stopped collecing stats for container %s", container.Name)

		daemon.statsLoggers.Lock()
		delete(daemon.statsLoggers.m, container.ID)
		daemon.statsLoggers.Unlock()
	} else {
		daemon.statsLoggers.RUnlock()
	}
}

func (daemon *Daemon) StartStatsLogs(container *container.Container) {
	folderpath := "/var/logs/docker/stats"
	os.MkdirAll(folderpath, os.ModePerm)
	targetFilepath := filepath.Join(folderpath, container.ID+".log")

	mode := os.O_APPEND | os.O_CREATE | os.O_WRONLY
	if file, err := os.OpenFile(targetFilepath, mode, 0644); err == nil {
		// Close file after container stats finishes
		defer file.Close()

		config := &backend.ContainerStatsConfig{
			Stream: true,
			// Use the file as the outstream (instead of an http stream)
			OutStream: file,
			Buffer:    true,
			Format:    backend.ContainerStatsFormatCsv,
			Version:   api.DefaultVersion,
		}

		logrus.Debugf("Collecing stats for container %s at a %dms interval in %s",
			container.Name, daemon.configStore.StatsInterval, targetFilepath)

		stat_context, cancel := context.WithCancel(context.Background())
		daemon.statsLoggers.Lock()
		daemon.statsLoggers.m[container.ID] = cancel
		daemon.statsLoggers.Unlock()

		daemon.ContainerStats(stat_context, container.ID, config)
	} else {
		logrus.WithError(err).WithField("container", container.ID).Errorf("Error opening file '%s' for stats logging", targetFilepath)
	}
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

	// Use a buffered writer to batch stream writes
	if config.Buffer {
		bufferedWriter := MakeBufferedWriter(outStream)
		defer bufferedWriter.Flush()
		outStream = bufferedWriter
	}

	var encode func(interface{}) error
	if config.Format == backend.ContainerStatsFormatCsv {
		if versions.LessThan(apiVersion, "1.21") {
			return errors.New("API versions pre v1.21 do not support writing to CSV")
		}

		csvEncoder := csv.NewWriter(outStream)
		defer csvEncoder.Flush()

		// write the initial header row
		// Note: important to be in exactly the same order as the encode function
		//       below writes it
		header := [...]string{
			"read",
			"preread",
			"name",
			"id",
			"cpu_stats.cpu_usage.total_usage",
			"cpu_stats.cpu_usage.percpu_usage",
			"cpu_stats.cpu_usage.usage_in_kernelmode",
			"cpu_stats.cpu_usage.usage_in_usermode",
			"cpu_stats.system_cpu_usage",
			"cpu_stats.online_cpus",
			"cpu_stats.throttling_data.periods",
			"cpu_stats.throttling_data.throttled_periods",
			"cpu_stats.throttling_data.throttled_time",
			"memory_stats.usage",
			"memory_stats.max_usage",
			"memory_stats.stats",
			"memory_stats.failcnt",
			"memory_stats.limit",
			"memory_stats.commitbytes",
			"memory_stats.commitpeakbytes",
			"memory_stats.privateworkingset",
			"pids_stats.current",
			"pids_stats.limit",
			"num_procs",
			"storage_stats.read_count_normalized",
			"storage_stats.read_size_bytes",
			"storage_stats.write_count_normalized",
			"storage_stats.write_size_bytes",
			"blkio_stats.io_service_bytes_recursive",
			"blkio_stats.io_serviced_recursive",
			"blkio_stats.io_queue_recursive",
			"blkio_stats.io_service_time_recursive",
			"blkio_stats.io_wait_time_recursive",
			"blkio_stats.io_merged_recursive",
			"blkio_stats.io_time_recursive",
			"blkio_stats.sectors_recursive",
			"networks",
		}
		csvEncoder.Write(header[:])

		encode = func(s interface{}) error {
			stats := *(s.(*types.StatsJSON))
			record := [...]string{
				int64ToString(stats.Read.UnixNano()),
				int64ToString(stats.PreRead.UnixNano()),
				stats.Name,
				stats.ID,
				uint64ToString(stats.CPUStats.CPUUsage.TotalUsage),
				uint64ArrayToString(stats.CPUStats.CPUUsage.PercpuUsage),
				uint64ToString(stats.CPUStats.CPUUsage.UsageInKernelmode),
				uint64ToString(stats.CPUStats.CPUUsage.UsageInUsermode),
				uint64ToString(stats.CPUStats.SystemUsage),
				uint32ToString(stats.CPUStats.OnlineCPUs),
				uint64ToString(stats.CPUStats.ThrottlingData.Periods),
				uint64ToString(stats.CPUStats.ThrottlingData.ThrottledPeriods),
				uint64ToString(stats.CPUStats.ThrottlingData.ThrottledTime),
				uint64ToString(stats.MemoryStats.Usage),
				uint64ToString(stats.MemoryStats.MaxUsage),
				mapStringUint64ToString(&stats.MemoryStats.Stats),
				uint64ToString(stats.MemoryStats.Failcnt),
				uint64ToString(stats.MemoryStats.Limit),
				uint64ToString(stats.MemoryStats.Commit),
				uint64ToString(stats.MemoryStats.CommitPeak),
				uint64ToString(stats.MemoryStats.PrivateWorkingSet),
				uint64ToString(stats.PidsStats.Current),
				uint64ToString(stats.PidsStats.Limit),
				uint32ToString(stats.NumProcs),
				uint64ToString(stats.StorageStats.ReadCountNormalized),
				uint64ToString(stats.StorageStats.ReadSizeBytes),
				uint64ToString(stats.StorageStats.WriteCountNormalized),
				uint64ToString(stats.StorageStats.WriteSizeBytes),
				blkioArrayToString(&stats.BlkioStats.IoServiceBytesRecursive),
				blkioArrayToString(&stats.BlkioStats.IoServicedRecursive),
				blkioArrayToString(&stats.BlkioStats.IoQueuedRecursive),
				blkioArrayToString(&stats.BlkioStats.IoServiceTimeRecursive),
				blkioArrayToString(&stats.BlkioStats.IoWaitTimeRecursive),
				blkioArrayToString(&stats.BlkioStats.IoMergedRecursive),
				blkioArrayToString(&stats.BlkioStats.IoTimeRecursive),
				blkioArrayToString(&stats.BlkioStats.SectorsRecursive),
				networkStatsToString(&stats.Networks),
			}

			return csvEncoder.Write(record[:])
		}
	} else {
		jsonEncoder := json.NewEncoder(outStream)
		encode = func(stats interface{}) error {
			return jsonEncoder.Encode(stats)
		}
	}

	updates := daemon.subscribeToContainerStats(container)
	defer daemon.unsubscribeToContainerStats(container, updates)

	noStreamFirstFrame := true
	for {
		select {
		case v, ok := <-updates:
			if !ok {
				return nil
			}

			if !config.Stream && noStreamFirstFrame {
				// prime the cpu stats so they aren't 0 in the final output
				noStreamFirstFrame = false
				continue
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

			if err := encode(statsJSON); err != nil {
				return err
			}

			if !config.Stream {
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

// io.Writer that includes a large buffer and only writes once the buffer is full,
// or upon a call to writer.Flush(). Useful in the modifications for batching CSV writes
// to a file to reduce I/O overhead and instead write in memory
type BufferedWriter struct {
	Buffer [BUFFER_LENGTH]byte
	N      int
	Size   int
	Writer io.Writer
}

func MakeBufferedWriter(dest io.Writer) *BufferedWriter {
	return &BufferedWriter{Writer: dest, Size: BUFFER_LENGTH}
}

func (w *BufferedWriter) Write(p []byte) (int, error) {
	originalLength := len(p)
	srcRemaining := originalLength
	srcStart := 0

	// Keep writing and flushing until remaining can fit
	for srcRemaining+w.N > w.Size {
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

// ? =====================================================
// ? Utility ToString functions for generating CSV records
// ? =====================================================

func uint64ToString(u uint64) string {
	return strconv.FormatUint(u, 10)
}

func int64ToString(i int64) string {
	return strconv.FormatInt(i, 10)
}

func uint32ToString(u uint32) string {
	return uint64ToString(uint64(u))
}

func uint64ArrayToString(a []uint64) string {
	if len(a) == 0 {
		return ""
	}

	var str strings.Builder
	last := len(a) - 1
	for i, v := range a {
		str.WriteString(strconv.FormatUint(v, 10))
		if i != last {
			str.WriteRune(',')
		}
	}
	return str.String()
}

func mapStringUint64ToString(m *map[string]uint64) string {
	var str strings.Builder
	str.WriteRune('{')
	for key, element := range *m {
		str.WriteString(key)
		str.WriteRune(':')
		str.WriteString(strconv.FormatUint(element, 10))
		str.WriteRune(',')
	}
	str.WriteRune('}')
	return str.String()
}

func blkioArrayToString(a *[]types.BlkioStatEntry) string {
	if len(*a) == 0 {
		return ""
	}

	var str strings.Builder
	last := len(*a) - 1
	for i, v := range *a {
		str.WriteString(strconv.FormatUint(v.Major, 10))
		str.WriteRune(' ')
		str.WriteString(strconv.FormatUint(v.Minor, 10))
		str.WriteRune(' ')
		str.WriteString(strconv.FormatUint(v.Value, 10))
		str.WriteRune(' ')
		str.WriteString(v.Op)
		if i != last {
			str.WriteRune(',')
		}
	}
	return str.String()
}

func networkStatsToString(n *map[string]types.NetworkStats) string {
	var str strings.Builder
	str.WriteRune('{')
	for key, element := range *n {
		str.WriteString(key)
		str.WriteString(":\"")
		writeNetworkStats(&element, &str)
		str.WriteString("\",")
	}
	str.WriteRune('}')
	return str.String()
}

func writeNetworkStats(n *types.NetworkStats, str *strings.Builder) {
	str.WriteString(strconv.FormatUint(n.RxBytes, 10))
	str.WriteRune(' ')
	str.WriteString(strconv.FormatUint(n.RxPackets, 10))
	str.WriteRune(' ')
	str.WriteString(strconv.FormatUint(n.RxErrors, 10))
	str.WriteRune(' ')
	str.WriteString(strconv.FormatUint(n.RxDropped, 10))
	str.WriteRune('|')
	str.WriteString(strconv.FormatUint(n.TxBytes, 10))
	str.WriteRune(' ')
	str.WriteString(strconv.FormatUint(n.TxPackets, 10))
	str.WriteRune(' ')
	str.WriteString(strconv.FormatUint(n.TxErrors, 10))
	str.WriteRune(' ')
	str.WriteString(strconv.FormatUint(n.TxDropped, 10))
}
