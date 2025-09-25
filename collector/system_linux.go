/*
 * Copyright 2025 RapidLoop, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package collector

import (
	"bufio"
	"bytes"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/rapidloop/pgmetrics"
)

func (c *collector) collectSystem(o CollectConfig) {
	c.result.System = &pgmetrics.SystemMetrics{}

	// 1. disk space (bytes free/used/reserved, inodes free/used) for each tablespace
	for i := range c.result.Tablespaces {
		c.doStatFS(&c.result.Tablespaces[i])
	}

	// 2. cpu model, core count
	c.getCPUs()

	// 3. load average
	c.getLoadAvg()

	// 4. memory info: used, free, buffers, cached; swapused, swapfree
	c.getMemory()

	// 5. hostname
	c.result.System.Hostname, _ = os.Hostname()
}

func (c *collector) doStatFS(t *pgmetrics.Tablespace) {
	path := t.Location
	if len(path) == 0 {
		return
	}
	var buf syscall.Statfs_t
	if err := syscall.Statfs(path, &buf); err != nil {
		return // ignore errors, not fatal
	}
	t.DiskUsed = int64(buf.Bsize) * int64(buf.Blocks-buf.Bfree)
	t.DiskTotal = int64(buf.Bsize) * int64(buf.Blocks)
	t.InodesUsed = int64(buf.Files - buf.Ffree)
	t.InodesTotal = int64(buf.Files)
}

func (c *collector) getCPUs() {
	f, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "model name") {
			if pos := strings.Index(line, ":"); pos != -1 {
				c.result.System.CPUModel = strings.TrimSpace(line[pos+1:])
			}
			c.result.System.NumCores++
		}
	}
}

func (c *collector) getLoadAvg() {
	raw, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return
	}

	parts := strings.Fields(string(raw))
	if len(parts) != 5 {
		return
	}

	if v, err := strconv.ParseFloat(parts[0], 64); err == nil {
		c.result.System.LoadAvg = v
	}
}

func (c *collector) getMemory() {
	raw, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return
	}
	scanner := bufio.NewScanner(bytes.NewReader(raw))

	// scan it
	memInfo := make(map[string]int64)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 3 && fields[2] == "kB" {
			val, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				return
			}
			memInfo[fields[0]] = val * 1024
		}
	}

	// RAM
	c.result.System.MemFree = memInfo["MemFree:"]
	c.result.System.MemBuffers = memInfo["Buffers:"]
	c.result.System.MemCached = memInfo["Cached:"]
	c.result.System.MemSlab = memInfo["Slab:"]
	c.result.System.MemUsed = memInfo["MemTotal:"] - c.result.System.MemFree -
		c.result.System.MemBuffers - c.result.System.MemCached -
		c.result.System.MemSlab

	// Swap
	if val, ok := memInfo["SwapTotal:"]; ok {
		if val2, ok2 := memInfo["SwapFree:"]; ok2 {
			if val != 0 || val2 != 0 {
				c.result.System.SwapUsed = val - val2
				c.result.System.SwapFree = val2
			}
		}
	}
}
