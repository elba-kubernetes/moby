package daemon // import "github.com/docker/docker/daemon"

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/backend"
	"github.com/docker/docker/api/types/versions"
	"github.com/docker/docker/api/types/versions/v1p20"
	"github.com/docker/docker/container"
	"github.com/docker/docker/pkg/ioutils"
)

// Lines are ~1700 chars long, so this is ~10 lines at once
const BUFFER_LENGTH = 16384

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
		csvEncoder := csv.NewWriter(outStream)
		defer csvEncoder.Flush()

		// write the initial header row
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
			"pids_stats.current",
			"pids_stats.limit",
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

			read := strconv.FormatInt(stats.Read.UnixNano(), 10)
			preread := strconv.FormatInt(stats.PreRead.UnixNano(), 10)
			name := stats.Name
			id := stats.ID
			cpu_total := strconv.FormatUint(stats.CPUStats.CPUUsage.TotalUsage, 10)
			cpu_per_core := UintToString(stats.CPUStats.CPUUsage.PercpuUsage, ",")
			cpu_kernel := strconv.FormatUint(stats.CPUStats.CPUUsage.UsageInKernelmode, 10)
			cpu_user := strconv.FormatUint(stats.CPUStats.CPUUsage.UsageInUsermode, 10)
			cpu_system := strconv.FormatUint(stats.CPUStats.SystemUsage, 10)
			cpu_online_cpus := strconv.FormatUint(uint64(stats.CPUStats.OnlineCPUs), 10)
			cpu_throttiling_periods := strconv.FormatUint(stats.CPUStats.ThrottlingData.Periods, 10)
			cpu_throttiling_throttled_periods := strconv.FormatUint(stats.CPUStats.ThrottlingData.ThrottledPeriods, 10)
			cpu_throttiling_throttled_time := strconv.FormatUint(stats.CPUStats.ThrottlingData.ThrottledTime, 10)
			memory_usage := strconv.FormatUint(stats.MemoryStats.Usage, 10)
			memory_max_usage := strconv.FormatUint(stats.MemoryStats.MaxUsage, 10)
			memory_stats, _ := json.Marshal(stats.MemoryStats.Stats)
			memory_failcnt := strconv.FormatUint(stats.MemoryStats.Failcnt, 10)
			memory_limit := strconv.FormatUint(stats.MemoryStats.Limit, 10)
			pid_current := strconv.FormatUint(stats.PidsStats.Current, 10)
			pid_limit := strconv.FormatUint(stats.PidsStats.Limit, 10)
			io_service_bytes_recursive := IoToString(&stats.BlkioStats.IoServiceBytesRecursive)
			io_serviced_recursive := IoToString(&stats.BlkioStats.IoServicedRecursive)
			io_queue_recursive := IoToString(&stats.BlkioStats.IoQueuedRecursive)
			io_service_time_recursive := IoToString(&stats.BlkioStats.IoServiceTimeRecursive)
			io_wait_time_recursive := IoToString(&stats.BlkioStats.IoWaitTimeRecursive)
			io_merged_recursive := IoToString(&stats.BlkioStats.IoMergedRecursive)
			io_time_recursive := IoToString(&stats.BlkioStats.IoTimeRecursive)
			sectors_recursive := IoToString(&stats.BlkioStats.SectorsRecursive)
			networks, _ := json.Marshal(stats.Networks)

			record := [...]string{
				read,
				preread,
				name,
				id,
				cpu_total,
				cpu_per_core,
				cpu_kernel,
				cpu_user,
				cpu_system,
				cpu_online_cpus,
				cpu_throttiling_periods,
				cpu_throttiling_throttled_periods,
				cpu_throttiling_throttled_time,
				memory_usage,
				memory_max_usage,
				string(memory_stats),
				memory_failcnt,
				memory_limit,
				pid_current,
				pid_limit,
				io_service_bytes_recursive,
				io_serviced_recursive,
				io_queue_recursive,
				io_service_time_recursive,
				io_wait_time_recursive,
				io_merged_recursive,
				io_time_recursive,
				sectors_recursive,
				string(networks),
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

func UintToString(a []uint64, sep string) string {
	if len(a) == 0 {
		return ""
	}

	var str strings.Builder
	last := len(a) - 1
	for i, v := range a {
		str.WriteString(strconv.FormatUint(v, 10))
		if i != last {
			str.WriteString(sep)
		}
	}
	return str.String()
}

func IoToString(a *[]types.BlkioStatEntry) string {
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
