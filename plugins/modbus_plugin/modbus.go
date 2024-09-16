package modbus_plugin

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/goburrow/modbus"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var ModbusConfigSpec = service.NewConfigSpec().
	Summary("Creates an Modbus output").
	Field(service.NewStringField("endpoint").Description("Address to connect")).
	Field(service.NewStringListField("subscriptions").Description("List of nodes like DB,group etc")).
	Field(service.NewIntField("timeout").Description("The timeout duration in seconds for connection attempts and read requests.").Default(10)).
	Field(service.NewIntField("slaveid").Description("SlaveID")).
	Field(service.NewBoolField("subscribeEnabled").Description("Set to true to subscribe").Default(true))

func newModbusoutput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	endpoint, err := conf.FieldString("endpoint")
	if err != nil {
		return nil, err
	}
	timeoutInt, err := conf.FieldInt("timeout")
	if err != nil {
		return nil, err
	}
	slaveid, err := conf.FieldInt("slaveid")
	if err != nil {
		return nil, err
	}
	subscriptions, err := conf.FieldStringList("subscriptions")
	if err != nil {
		return nil, err
	}
	subscribeEnabled, err := conf.FieldBool("subscribeEnabled")
	if err != nil {
		return nil, err
	}
	modbusInput := &modbusInput{
		endpoint:         endpoint,
		subscription:     ParseSubscriptionDef(subscriptions),
		slaveId:          slaveid,
		timeout:          time.Duration(timeoutInt) * time.Second,
		subscribeEnabled: subscribeEnabled,
	}
	return service.AutoRetryNacksBatched(modbusInput), nil
}
func init() {
	// Register our new output plugin
	err := service.RegisterBatchInput(
		"modbus", ModbusConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			mgr.Logger().Infof("Created & maintained by the BGRI ")
			return newModbusoutput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func (g *modbusInput) Connect(ctx context.Context) error {
	//client := modbus.TCPClient(g.endpoint)
	handler := modbus.NewTCPClientHandler(g.endpoint)
	handler.Timeout = 10 * time.Second
	//handler.SlaveId = 4
	handler.SlaveId = byte(g.slaveId)
	//handler.Logger = log.New(os.Stdout, "test: ", log.LstdFlags)
	// Connect manually so that multiple requests are handled in one connection session
	err := handler.Connect()
	if err != nil {
		return err
	}
	g.tcpHandler = handler
	client := modbus.NewClient(handler)
	g.client = client
	return nil
}
func (g *modbusInput) Close(ctx context.Context) error {
	if g.tcpHandler != nil {
		return g.tcpHandler.Close()
	}
	return nil

}
func (g *modbusInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if ctx == nil || ctx.Done() == nil {
		return nil, nil, errors.New("emptyCtx is invalid for ReadBatchSubscribe")
	}
	msgs := service.MessageBatch{}
	for i, subscription := range g.subscription {
		if subscription.addressType == TYPE_REGISTER {
			results, err := g.client.ReadInputRegisters(subscription.address, 1)
			if err != nil {
				return nil, func(ctx context.Context, err error) error {
					return nil // Acknowledgment handling here if needed
				}, err
			}
			data := binary.BigEndian.Uint16(results)

			value := int16(data)

			if g.subscription[i].value != value {
				log.Println("Register subscription.value:", subscription.value, " value:", value)
				msg := g.createMessageFromValue(value, subscription)
				msgs = append(msgs, msg)
				g.subscription[i].value = value
			}
		} else if subscription.addressType == TYPE_COIL {
			results, err := g.client.ReadCoils(subscription.address, 1)
			if err != nil {
				return nil, func(ctx context.Context, err error) error {
					return nil // Acknowledgment handling here if needed
				}, err
			}
			//data := binary.BigEndian.Uint16(results)
			if len(results) > 0 {
				value := int16(results[0])

				if g.subscription[i].value != value {
					log.Println("coil subscription.value:", subscription.value, " value:", value)
					msg := g.createMessageFromValue(value, subscription)
					msgs = append(msgs, msg)
					g.subscription[i].value = value
				}
			}

		} else if subscription.addressType == TYPE_DISCRETE {
			results, err := g.client.ReadDiscreteInputs(subscription.address, 1)
			if err != nil {
				return nil, func(ctx context.Context, err error) error {
					return nil // Acknowledgment handling here if needed
				}, err
			}
			//data := binary.BigEndian.Uint16(results)
			if len(results) > 0 {
				value := int16(results[0])
				if g.subscription[i].value != value {
					log.Println("coil subscription.value:", subscription.value, " value:", value)
					msg := g.createMessageFromValue(value, subscription)
					msgs = append(msgs, msg)
					g.subscription[i].value = value
				}
			}

		} else if subscription.addressType == TYPE_HOLDING {
			results, err := g.client.ReadHoldingRegisters(subscription.address, 1)
			if err != nil {
				return nil, func(ctx context.Context, err error) error {
					return nil // Acknowledgment handling here if needed
				}, err
			}
			data := binary.BigEndian.Uint16(results)

			value := int16(data)

			if g.subscription[i].value != value {
				log.Println("Holding subscription.value:", subscription.value, " value:", value)
				msg := g.createMessageFromValue(value, subscription)
				msgs = append(msgs, msg)
				g.subscription[i].value = value
			}
		}

	}
	return msgs, func(ctx context.Context, err error) error {
		return nil // Acknowledgment handling here if needed
	}, nil
}
func (g *modbusInput) createMessageFromValue(value int16, subscription subscriptionDef) *service.Message {

	message := service.NewMessage(nil)
	log.Println("Value:", fmt.Sprintf("%d", value))
	message.MetaSet("value", fmt.Sprintf("%d", value))
	message.MetaSet("db", subscription.DB)
	message.MetaSet("name", subscription.Name)
	message.MetaSet("group", subscription.Group)
	message.MetaSet("historian", subscription.Historian)
	message.MetaSet("sqlSp", subscription.SqlSp)

	return message

}
