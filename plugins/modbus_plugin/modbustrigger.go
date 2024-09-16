package modbus_plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/goburrow/modbus"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var ModbusTriggerConfigSpec = service.NewConfigSpec().
	Summary("Creates an Modbus output").
	Field(service.NewStringField("endpoint").Description("Address to connect")).
	Field(service.NewStringListField("subscriptions").Description("List of AB addresses Address formats include direct area access")).
	Field(service.NewStringListField("tsubscriptions").Description("List of AB trigger node IDs.")).
	Field(service.NewIntField("timeout").Description("The timeout duration in seconds for connection attempts and read requests.").Default(10)).
	Field(service.NewIntField("slaveid").Description("SlaveID")).
	Field(service.NewBoolField("subscribeEnabled").Description("Set to true to subscribe").Default(true))

func newModbusTriggerOutput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
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

	tsubscriptions, err := conf.FieldStringList("tsubscriptions")
	if err != nil {
		return nil, err
	}
	subscribeEnabled, err := conf.FieldBool("subscribeEnabled")
	if err != nil {
		return nil, err
	}
	if len(tsubscriptions) != len(subscriptions) {
		return nil, errors.New("subscription and tsubscription fields must be the same length")
	}
	tSub := ParseTSubscription(tsubscriptions)

	modbusTriggerInput := &modbusTriggerInput{
		endpoint:         endpoint,
		subscription:     ParseSubscriptionDef(subscriptions),
		tSubscription:    tSub,
		slaveId:          slaveid,
		timeout:          time.Duration(timeoutInt) * time.Second,
		subscribeEnabled: subscribeEnabled,
	}
	return service.AutoRetryNacksBatched(modbusTriggerInput), nil
}
func init() {
	// Register our new output plugin
	err := service.RegisterBatchInput(
		"modbustrigger", ModbusTriggerConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			mgr.Logger().Infof("Created & maintained by the BGRI ")
			return newModbusTriggerOutput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func (g *modbusTriggerInput) Connect(ctx context.Context) error {
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
func (g *modbusTriggerInput) Close(ctx context.Context) error {
	if g.tcpHandler != nil {
		return g.tcpHandler.Close()
	}
	return nil

}
func (g *modbusTriggerInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if ctx == nil || ctx.Done() == nil {
		return nil, nil, errors.New("emptyCtx is invalid for ReadBatchSubscribe")
	}
	msgs := service.MessageBatch{}
	for i, subscription := range g.subscription {
		value, err := readModbusValue(g.client, subscription.address, 1, subscription.addressType)
		if err != nil {
			//log.Printf("error reading %s: %v", subs.Address, err)
			return nil, func(ctx context.Context, err error) error {
				return nil // Acknowledgment handling here if needed
			}, err
		}
		subscription.value = value
		if g.subscription[i].value != subscription.value {

			msgsV := make(map[string]int16, 0)
			for _, tsubs := range g.tSubscription[i].tSub {
				log.Println("name:", tsubs.Name, " subscription.address:", subscription.address, " subscription.addressType:", subscription.addressType)
				reading, err := readModbusValue(g.client, tsubs.Address, 1, tsubs.AddressType)
				if err != nil {
					//log.Printf("error reading %s: %v", subs.Address, err)
					return nil, func(ctx context.Context, err error) error {
						return nil // Acknowledgment handling here if needed
					}, err
				}
				tsubs.Value = reading
				msgsV[tsubs.Name] = reading

			}

			msg := g.createMessageFromValue(subscription, msgsV)
			msgs = append(msgs, msg)
			g.subscription[i] = subscription
		}
	}
	return msgs, func(ctx context.Context, err error) error {
		return nil // Acknowledgment handling here if needed
	}, nil
}
func (g *modbusTriggerInput) createMessageFromValue(node subscriptionDef, messageJ map[string]int16) *service.Message {
	re := regexp.MustCompile(`[^a-zA-Z0-9_-]`)
	message := service.NewMessage(nil)
	log.Println("Value:", fmt.Sprintf("%d", node.value))
	message.MetaSet("value", fmt.Sprintf("%d", node.value))
	message.MetaSet("db", node.DB)
	message.MetaSet("name", node.Name)
	message.MetaSet("group", node.Group)
	message.MetaSet("historian", node.Historian)
	message.MetaSet("sqlSp", node.SqlSp)
	newAddress := make(map[string]int16)
	for address, val := range messageJ {
		addressName := re.ReplaceAllString(address, "_")
		newAddress[addressName] = val
	}

	jsonMsg, err := json.Marshal(newAddress)
	if err != nil {
		g.log.Errorf("Could not change benthos message to json object")
		return nil
	}
	message.MetaSet("Message", string(jsonMsg))
	return message

}
