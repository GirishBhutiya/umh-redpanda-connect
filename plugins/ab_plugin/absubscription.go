package ab_plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/danomagnum/gologix"
	"github.com/redpanda-data/benthos/v4/public/service"
)

//------------------------------------------------------------------------------

// S7CommInput struct defines the structure for our custom Benthos input plugin.
// It holds the configuration necessary to establish a connection with a Siemens S7 PLC,
// along with the read requests to fetch data from the PLC.
type ABCommInputSub struct {
	tcpDevice    string        // IP address of the S7 PLC.
	timeout      time.Duration // Time duration before a connection attempt or read request times out.
	subscription []subscriptionD
	log          *service.Logger // Logger for logging plugin activity.
	client       *gologix.Client
	//OldSub        subscriptionDef
}
type subscriptionD struct {
	ID        int
	Address   string
	Name      string
	Group     string
	DB        string
	Historian string
	SqlSp     string
	DataType  string
	Value     any
}

func ParseSubscriptionDef(subscription []string) []subscriptionD {
	var parsedSubscription []subscriptionD

	for _, subscriptionElement := range subscription {
		var subscr map[string][]map[string]string
		var subsc subscriptionD
		err := json.Unmarshal([]byte(subscriptionElement), &subscr)
		if err != nil {
			return nil
		}
		//log.Println("Girish ParseSubscription() json tagname: ", subscr)
		for key, values := range subscr {
			for _, obj := range values {

				subsc.ID, _ = strconv.Atoi(key)
				subsc.Address = obj["address"]
				subsc.Group = obj["group"]
				subsc.DB = obj["db"]
				subsc.Historian = obj["historian"]
				subsc.SqlSp = obj["sqlSp"]
				subsc.DataType = obj["datatype"]
				subsc.Name = obj["name"]

			}
		}
		parsedSubscription = append(parsedSubscription, subsc)
	}
	return parsedSubscription

}

// S7CommConfigSpec defines the configuration options available for the S7CommInput plugin.
// It outlines the required information to establish a connection with the PLC and the data to be read.
var ABCommSubInputCommConfigSpec = service.NewConfigSpec().
	Summary("Creates an input that reads data from Allen Bradley PLCs. Created & maintained by the").
	Description("This input plugin enables Benthos to read data directly from Allen Bradly PLCs" +
		"Configure the plugin by specifying the PLC's IP address, rack and slot numbers, and the data blocks to read.").
	Field(service.NewStringField("tcpDevice").Description("IP address of the Allen Bradly PLC.")).
	Field(service.NewIntField("timeout").Description("The timeout duration in seconds for connection attempts and read requests.").Default(10)).
	Field(service.NewStringListField("subscriptions").Description("List of AB addresses Address formats include direct area access"))

// newS7CommInput is the constructor function for S7CommInput. It parses the plugin configuration,
// establishes a connection with the S7 PLC, and initializes the input plugin instance.
func newABCommSubInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {

	tcpDevice, err := conf.FieldString("tcpDevice")
	if err != nil {
		return nil, err
	}

	timeoutInt, err := conf.FieldInt("timeout")
	if err != nil {
		return nil, err
	}

	subscriptions, err := conf.FieldStringList("subscriptions")
	if err != nil {
		return nil, err
	}

	sub := ParseSubscriptionDef(subscriptions)

	m := &ABCommInputSub{
		tcpDevice:    tcpDevice,
		subscription: sub,
		log:          mgr.Logger(),
		timeout:      time.Duration(timeoutInt) * time.Second,
	}

	return service.AutoRetryNacksBatched(m), nil
}

//------------------------------------------------------------------------------

func init() {

	err := service.RegisterBatchInput(
		"absubscription", ABCommSubInputCommConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			mgr.Logger().Infof("Created & maintained by the BGRI ")
			return newABCommSubInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func (g *ABCommInputSub) Connect(ctx context.Context) error {

	if g.client != nil {
		return nil
	}
	client := gologix.NewClient(g.tcpDevice)
	err := client.Connect()
	if err != nil {
		//log.Printf("Error opening client. %v", err)
		return err
	}
	g.client = client
	return nil
}

func (g *ABCommInputSub) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if ctx == nil || ctx.Done() == nil {
		return nil, nil, errors.New("emptyCtx is invalid for ReadBatchSubscribe")
	}

	msgs := service.MessageBatch{}
	for i, subs := range g.subscription {

		value, err := g.client.Read_single(subs.Address, gologix.CIPTypeUnknown, 1)
		if err != nil {
			//log.Printf("error reading %s: %v", subs.Address, err)
			return nil, func(ctx context.Context, err error) error {
				return nil // Acknowledgment handling here if needed
			}, err
		}

		if subs.DataType == "str" {
			v, ok := value.([]byte)
			if !ok {
				log.Println("Can not convert to byte")
			}
			subs.Value = string(v)

		} else {
			subs.Value = value
		}
		//log.Println("current str value:", g.subscription[i].Value, " New Value:", subs.Value, " Address:", subs.Address, "comparission:", !reflect.DeepEqual(g.subscription[i].Value, subs.Value))

		if !reflect.DeepEqual(g.subscription[i].Value, subs.Value) {

			if subs.DataType == "str" {
				v, ok := value.([]byte)
				if !ok {
					log.Println("Can not convert to byte")
				}
				subs.Value = string(v)

			} else {
				subs.Value = value
			}
			val := fmt.Sprint(subs.Value)
			msg := g.createMessageFromValue(subs, strings.TrimSpace(val))
			msgs = append(msgs, msg)
			g.subscription[i] = subs
		}

	}

	return msgs, func(ctx context.Context, err error) error {
		return nil // Acknowledgment handling here if needed
	}, nil
}

func (g *ABCommInputSub) Close(ctx context.Context) error {

	if g.client != nil {
		g.client.Disconnect()
	}
	return nil
}

// createMessageFromValue creates a benthos messages from a given variant and nodeID
// theoretically nodeID can be extracted from variant, but not in all cases (e.g., when subscribing), so it it left to the calling function
func (g *ABCommInputSub) createMessageFromValue(subscriptionD subscriptionD, tagValue string) *service.Message {

	//log.Println("value is:", cleanSubString(tagValue), "L")
	message := service.NewMessage(nil)
	message.MetaSet("value", cleanSubString(tagValue))
	message.MetaSet("tag_name", subscriptionD.Address)
	message.MetaSet("group", subscriptionD.Group)
	message.MetaSet("db", subscriptionD.DB)
	message.MetaSet("historian", subscriptionD.Historian)
	message.MetaSet("sqlSp", subscriptionD.SqlSp)
	message.MetaSet("datatype", subscriptionD.DataType)
	trigMap := make(map[string]string)
	trigMap[subscriptionD.Name] = cleanSubString(tagValue)
	jsonMsg, err := json.Marshal(trigMap)
	if err != nil {
		g.log.Errorf("Could not change benthos message to json object")
		return nil
	}
	message.MetaSet("Message", string(jsonMsg))

	return message

}
func cleanSubString(str string) string {
	re := regexp.MustCompile("[\b\a\x00 ]+") //split according to \s, \t, \r, \t and whitespace. Edit this regex for other 'conditions'

	split := re.ReplaceAllLiteralString(str, "")
	return split
}
