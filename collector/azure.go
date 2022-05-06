package collector

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/monitor/armmonitor"
	"github.com/rapidloop/pgmetrics"
)

const (
	flexibleServerMetrics = `backup_storage_used,cpu_percent,memory_percent,iops,disk_queue_depth,read_throughput,write_throughput,read_iops,write_iops,storage_percent,storage_used,storage_free,txlogs_storage_used,active_connections,network_bytes_egress,network_bytes_ingress,connections_failed,connections_succeeded,maximum_used_transactionIDs`
	singleServerMetrics   = `cpu_percent,memory_percent,io_consumption_percent,storage_percent,storage_used,storage_limit,serverlog_storage_percent,serverlog_storage_usage,serverlog_storage_limit,active_connections,connections_failed,backup_storage_used,network_bytes_egress,network_bytes_ingress,pg_replica_log_delay_in_seconds,pg_replica_log_delay_in_bytes`
	citusMetrics          = `cpu_percent,memory_percent,apps_reserved_memory_percent,iops,storage_percent,storage_used,active_connections,network_bytes_egress,network_bytes_ingress`
)

var rxResource = regexp.MustCompile(`(?i)^/subscriptions/([^/]{36})/resourceGroups/([^/]+)/providers/Microsoft.DBforPostgreSQL/(flexibleServers|servers|serverGroupsv2)/([^/]+)$`)

func collectAzure(ctx context.Context, resourceID string, out *pgmetrics.Azure) error {

	// parse resource URI
	m := rxResource.FindStringSubmatch(resourceID)
	if len(m) != 5 {
		return errors.New("invalid resource ID")
	}
	out.ResourceType = "Microsoft.DBforPostgreSQL/" + m[3]
	out.ResourceName = m[4]
	out.Metrics = make(map[string]float64)

	// get credentials
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return fmt.Errorf("failed to get credentials: %v", err)
	}

	// create a client
	client, err := armmonitor.NewMetricsClient(cred, nil)
	if err != nil {
		return fmt.Errorf("failed to create client: %v", err)
	}

	// make parameters for query
	to := time.Now().In(time.UTC)
	from := to.Add(-5 * time.Minute)
	timeRange := from.Format(time.RFC3339) + "/" + to.Format(time.RFC3339)
	var interval string
	var top int32 = 1
	var metricNames string
	switch m[3] {
	case "flexibleServers":
		interval = "PT1M"
		metricNames = flexibleServerMetrics
	case "servers":
		interval = "PT15M"
		metricNames = singleServerMetrics
	case "serverGroupsv2":
		interval = "PT1M"
		metricNames = citusMetrics
	}

	// actually query
	resp, err := client.List(
		ctx,
		resourceID,
		&armmonitor.MetricsClientListOptions{
			Interval:    &interval,
			Metricnames: &metricNames,
			Timespan:    &timeRange,
			Top:         &top,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to query Azure API: %v", err)
	}

	// parse response
	out.ResourceRegion = *resp.Resourceregion
	for _, m := range resp.Value {
		if m.ID == nil || *m.ID == "" {
			// log.Printf("warning: ignoring metric with no id: %v", m)
			continue
		}
		if len(m.Timeseries) == 0 {
			continue // no timeseries data, ignore quietly
		}
		ts := m.Timeseries[len(m.Timeseries)-1]
		if len(ts.Data) == 0 {
			continue // no timeseries data, ignore quietly
		}
		name := azGetMetricName(*m.ID)
		for i := len(ts.Data) - 1; i >= 0; i-- {
			t := ts.Data[i]
			if t.TimeStamp == nil {
				continue
			}
			if t.Average != nil {
				out.Metrics[name] = *t.Average
				break
			}
			if t.Total != nil {
				out.Metrics[name] = *t.Total
				break
			}
			if t.Maximum != nil {
				out.Metrics[name] = *t.Maximum
				break
			}
		}
	}

	return nil
}

func azGetMetricName(id string) string {
	i := strings.LastIndexByte(id, '/')
	if i != -1 {
		return id[int(i)+1:]
	}
	return id
}
