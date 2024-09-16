// Copyright 2024 UMH Systems GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s7comm_plugin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/robinson/gos7" // gos7 is a Go client library for interacting with Siemens S7 PLCs.
)

//------------------------------------------------------------------------------

// S7CommInput struct defines the structure for our custom Benthos input plugin.
// It holds the configuration necessary to establish a connection with a Siemens S7 PLC,
// along with the read requests to fetch data from the PLC.
type S7CommInput struct {
	tcpDevice    string                                // IP address of the S7 PLC.
	rack         int                                   // Rack number where the CPU resides. Identifies the physical location within the PLC rack.
	slot         int                                   // Slot number where the CPU resides. Identifies the CPU slot within the rack.
	batchMaxSize int                                   // Maximum count of addresses to be bundled in one batch-request. Affects PDU size.
	timeout      time.Duration                         // Time duration before a connection attempt or read request times out.
	client       gos7.Client                           // S7 client for communication.
	handler      *gos7.TCPClientHandler                // TCP handler to manage the connection.
	log          *service.Logger                       // Logger for logging plugin activity.
	batches      [][]S7DataItemWithAddressAndConverter // List of items to read from the PLC, grouped into batches with a maximum size.
	subscription []subscriptionD
}

type converterFunc func([]byte) interface{}

// S7CommConfigSpec defines the configuration options available for the S7CommInput plugin.
// It outlines the required information to establish a connection with the PLC and the data to be read.
var S7CommConfigSpec = service.NewConfigSpec().
	Summary("Creates an input that reads data from Siemens S7 PLCs. Created & maintained by the United Manufacturing Hub. About us: www.umh.app").
	Description("This input plugin enables Benthos to read data directly from Siemens S7 PLCs using the S7comm protocol. " +
		"Configure the plugin by specifying the PLC's IP address, rack and slot numbers, and the data blocks to read.").
	Field(service.NewStringField("tcpDevice").Description("IP address of the S7 PLC.")).
	Field(service.NewIntField("rack").Description("Rack number of the PLC. Identifies the physical location of the CPU within the PLC rack.").Default(0)).
	Field(service.NewIntField("slot").Description("Slot number of the PLC. Identifies the CPU slot within the rack.").Default(1)).
	Field(service.NewIntField("batchMaxSize").Description("Maximum count of addresses to be bundled in one batch-request (PDU size).").Default(480)).
	Field(service.NewIntField("timeout").Description("The timeout duration in seconds for connection attempts and read requests.").Default(10)).
	Field(service.NewStringListField("subscriptions").Description("List of S7 addresses to read in the format '<area>.<type><address>[.extra]', e.g., 'DB5.X3.2', 'DB5.B3', or 'DB5.C3'. " +
		"Address formats include direct area access (e.g., DB1 for data block one) and data types (e.g., X for bit, B for byte)."))

// newS7CommInput is the constructor function for S7CommInput. It parses the plugin configuration,
// establishes a connection with the S7 PLC, and initializes the input plugin instance.
func newS7CommInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	tcpDevice, err := conf.FieldString("tcpDevice")
	if err != nil {
		return nil, err
	}

	rack, err := conf.FieldInt("rack")
	if err != nil {
		return nil, err
	}

	slot, err := conf.FieldInt("slot")
	if err != nil {
		return nil, err
	}

	subscriptions, err := conf.FieldStringList("subscriptions")
	if err != nil {
		return nil, err
	}

	batchMaxSize, err := conf.FieldInt("batchMaxSize")
	if err != nil {
		return nil, err
	}

	timeoutInt, err := conf.FieldInt("timeout")
	if err != nil {
		return nil, err
	}

	// Now split the addresses into batches based on the batchMaxSize
	parsedSubscriptions, batches, err := ParseSubscriptionDef(subscriptions, batchMaxSize)
	if err != nil {
		return nil, err
	}

	m := &S7CommInput{
		tcpDevice:    tcpDevice,
		rack:         rack,
		slot:         slot,
		log:          mgr.Logger(),
		batches:      batches,
		subscription: parsedSubscriptions,
		batchMaxSize: batchMaxSize,
		timeout:      time.Duration(timeoutInt) * time.Second,
	}

	return service.AutoRetryNacksBatched(m), nil
}

//------------------------------------------------------------------------------

func init() {
	err := service.RegisterBatchInput(
		"s7comm", S7CommConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return newS7CommInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func (g *S7CommInput) Connect(ctx context.Context) error {
	g.handler = gos7.NewTCPClientHandler(g.tcpDevice, g.rack, g.slot)
	g.handler.Timeout = g.timeout
	g.handler.IdleTimeout = g.timeout

	err := g.handler.Connect()
	if err != nil {
		g.log.Errorf("Failed to connect to S7 PLC at %s: %v", g.tcpDevice, err)
		return err
	}

	g.client = gos7.NewClient(g.handler)
	g.log.Infof("Successfully connected to S7 PLC at %s", g.tcpDevice)

	cpuInfo, err := g.client.GetCPUInfo()
	if err != nil {
		g.log.Errorf("Failed to get CPU information: %v", err)
	} else {
		g.log.Infof("CPU Information: %s", cpuInfo)
	}

	return nil
}

func (g *S7CommInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if g.client == nil {
		return nil, nil, fmt.Errorf("S7Comm client is not initialized")
	}

	msgs := make(service.MessageBatch, 0)
	for i, b := range g.batches {

		// Create a new batch to read
		batchToRead := make([]gos7.S7DataItem, len(b))
		for i, item := range b {
			batchToRead[i] = item.Item
		}

		// Read the batch
		g.log.Debugf("Reading batch %d...", i+1)
		if err := g.client.AGReadMulti(batchToRead, len(batchToRead)); err != nil {
			// Try to reconnect and skip this gather cycle to avoid hammering
			// the network if the server is down or under load.
			errMsg := fmt.Sprintf("Failed to read batch %d: %v. Reconnecting...", i+1, err)

			// Return the error message so Benthos can handle it appropriately
			return nil, nil, errors.New(errMsg)
		}

		// Read the data from the batch and convert it using the converter function
		buffer := make([]byte, 0)

		for j, item := range b {
			// Execute the converter function to get the converted data
			convertedData := item.ConverterFunc(item.Item.Data)

			// Convert any type of convertedData to a string.
			// The fmt.Sprintf function is used here for its ability to handle various types gracefully.
			dataAsString := fmt.Sprintf("%v", convertedData)

			// Convert the string representation to a []byte
			dataAsBytes := []byte(dataAsString)

			// Append the converted data as bytes to the buffer
			buffer = append(buffer, dataAsBytes...)

			// Create a new message with the current state of the buffer
			// Note: Depending on your requirements, you may want to reset the buffer
			// after creating each message or keep accumulating data in it.
			//msg := service.NewMessage(buffer)
			if !bytes.Equal(item.oldValue, buffer) {
				msg := g.createMessageFromValue(g.subscription[i], buffer)
				// Append the new message to the msgs slice
				msgs = append(msgs, msg)
				g.batches[i][j].oldValue = buffer
			}

		}
	}

	return msgs, func(ctx context.Context, err error) error {
		return nil // Acknowledgment handling here if needed
	}, nil
}

// createMessageFromValue creates a benthos messages from a given variant and nodeID
// theoretically nodeID can be extracted from variant, but not in all cases (e.g., when subscribing), so it it left to the calling function
func (g *S7CommInput) createMessageFromValue(subscription subscriptionD, value []byte) *service.Message {

	//log.Println("value is:", cleanSubString(tagValue), "L")
	message := service.NewMessage(value)
	message.MetaSet("tag_name", subscription.Address)
	message.MetaSet("group", subscription.Group)
	message.MetaSet("db", subscription.DB)
	message.MetaSet("historian", subscription.Historian)
	message.MetaSet("sqlSp", subscription.SqlSp)
	message.MetaSet("datatype", subscription.DataType)

	return message

}
func (g *S7CommInput) Close(ctx context.Context) error {
	if g.handler != nil {
		g.handler.Close()
		g.handler = nil
		g.client = nil
	}

	return nil
}
