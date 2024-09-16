package ab_plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strconv"
	"time"

	"github.com/danomagnum/gologix"
	"github.com/redpanda-data/benthos/v4/public/service"
)

//------------------------------------------------------------------------------

// S7CommInput struct defines the structure for our custom Benthos input plugin.
// It holds the configuration necessary to establish a connection with a Siemens S7 PLC,
// along with the read requests to fetch data from the PLC.
type ABCommInput struct {
	tcpDevice     string        // IP address of the S7 PLC.
	timeout       time.Duration // Time duration before a connection attempt or read request times out.
	subscription  []subscriptionDef
	tSubscription []tSubscriptionsDef
	log           *service.Logger // Logger for logging plugin activity.
	client        *gologix.Client
	//OldSub        subscriptionDef
}
type subscriptionDef struct {
	ID        int
	Address   string
	Group     string
	DB        string
	Historian string
	SqlSp     string
	DataType  string
	Value     any
}
type tSubscriptionsDef struct {
	ID   int
	tSub []tSubscription
}
type tSubscription struct {
	Name     string
	Address  string
	DataType string
	Value    string
}

func ParseSubscription(subscription []string) []subscriptionDef {
	var parsedSubscription []subscriptionDef

	for _, subscriptionElement := range subscription {
		var subscr map[string][]map[string]string
		var subsc subscriptionDef
		err := json.Unmarshal([]byte(subscriptionElement), &subscr)
		if err != nil {
			return nil
		}
		//log.Println("Girish ParseSubscription() json tagname: ", subscr)
		for key, values := range subscr {
			for _, obj := range values {
				address := obj["address"]
				group := obj["group"]
				db := obj["db"]
				historian := obj["historian"]
				sqlSp := obj["sqlSp"]
				datatype := obj["datatype"]

				subsc.ID, _ = strconv.Atoi(key)
				subsc.Address = address
				subsc.Group = group
				subsc.DB = db
				subsc.Historian = historian
				subsc.SqlSp = sqlSp
				subsc.DataType = datatype

			}
		}
		parsedSubscription = append(parsedSubscription, subsc)
	}
	return parsedSubscription

}

func ParseTSubscription(tSubscriptions []string) []tSubscriptionsDef {
	var parsedtSubscription []tSubscriptionsDef

	for _, jsonString := range tSubscriptions {

		var temp map[string][]map[string]string

		// Unmarshal the JSON string into the temporary map
		err := json.Unmarshal([]byte(jsonString), &temp)
		if err != nil {
			//log.Println(err)
			return parsedtSubscription
		}
		var tSubsc tSubscriptionsDef
		// Merge the temporary map into the result map
		for key, values := range temp {
			var tsub tSubscription
			var subNodes []tSubscription
			// create node - name mapping for tbatch nodes
			for _, obj := range values {

				tsub.Address = obj["address"]
				tsub.Name = obj["name"]
				tsub.DataType = obj["datatype"]
				if err != nil {
					//log.Println(err)
					return parsedtSubscription
				}
				subNodes = append(subNodes, tsub)

			}
			tSubsc.ID, err = strconv.Atoi(key)
			tSubsc.tSub = subNodes
			//log.Println("Key:", key, "tSubsc: ", tSubsc)

			parsedtSubscription = append(parsedtSubscription, tSubsc)
		}
	}
	return parsedtSubscription
}

// S7CommConfigSpec defines the configuration options available for the S7CommInput plugin.
// It outlines the required information to establish a connection with the PLC and the data to be read.
var ABCommInputCommConfigSpec = service.NewConfigSpec().
	Summary("Creates an input that reads data from Allen Bradley PLCs. Created & maintained by the").
	Description("This input plugin enables Benthos to read data directly from Allen Bradly PLCs" +
		"Configure the plugin by specifying the PLC's IP address, rack and slot numbers, and the data blocks to read.").
	Field(service.NewStringField("tcpDevice").Description("IP address of the Allen Bradly PLC.")).
	Field(service.NewIntField("timeout").Description("The timeout duration in seconds for connection attempts and read requests.").Default(10)).
	Field(service.NewStringListField("subscriptions").Description("List of AB addresses Address formats include direct area access")).
	Field(service.NewStringListField("tsubscriptions").Description("List of AB trigger node IDs.")).
	Field(service.NewBoolField("insecure").Description("Set to true to bypass secure connections, useful in case of SSL or certificate issues. Default is secure (false).").Default(false)).
	Field(service.NewBoolField("subscribeEnabled").Description("Set to true to subscribe to AB nodes instead of fetching them every seconds. Default is pulling messages every second (false).").Default(false))

// newS7CommInput is the constructor function for S7CommInput. It parses the plugin configuration,
// establishes a connection with the S7 PLC, and initializes the input plugin instance.
func newABCommInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	//log.Println("Girish newABCommInput()")
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

	tsubscriptions, err := conf.FieldStringList("tsubscriptions")
	if err != nil {
		return nil, err
	}
	//log.Println(tsubscriptions)
	if len(tsubscriptions) != len(subscriptions) {
		return nil, errors.New("subscription and tsubscription fields must be the same length")
	}
	sub := ParseSubscription(subscriptions)

	tSub := ParseTSubscription(tsubscriptions)

	m := &ABCommInput{
		tcpDevice:     tcpDevice,
		subscription:  sub,
		tSubscription: tSub,
		log:           mgr.Logger(),
		timeout:       time.Duration(timeoutInt) * time.Second,
	}

	return service.AutoRetryNacksBatched(m), nil
}

//------------------------------------------------------------------------------

func init() {
	//log.Println("Girish init()")
	err := service.RegisterBatchInput(
		"abtrigger", ABCommInputCommConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			mgr.Logger().Infof("Created & maintained by the BGRI ")
			return newABCommInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func (g *ABCommInput) Connect(ctx context.Context) error {
	//log.Println("Girish Connect()")
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

func (g *ABCommInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
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

		/* if subs.DataType == "str" {
			v, ok := value.([]byte)
			if !ok {
				log.Println("Can not convert to byte1 ", value)
			}
			subs.Value = string(v)

		} else {
			subs.Value = value
		} */
		subs.Value = value
		//log.Println("current str value:", g.subscription[i].Value, " New Value:", subs.Value, " Address:", subs.Address, "comparission:", !reflect.DeepEqual(g.subscription[i].Value, subs.Value))

		if !reflect.DeepEqual(g.subscription[i].Value, subs.Value) {
			//log.Println("There is data change in address:", subs.Address)
			msgsV := make(map[string]string, 0)
			for _, tsubs := range g.tSubscription[i].tSub {
				tvalue, err := g.client.Read_single(tsubs.Address, gologix.CIPTypeUnknown, 1)
				if err != nil {
					//log.Printf("error reading %s: %v", subs.Address, err)
					return nil, func(ctx context.Context, err error) error {
						return nil // Acknowledgment handling here if needed
					}, err
				}
				tsubs.Value = fmt.Sprint(tvalue)

				if tsubs.DataType == "string" {
					v, ok := tvalue.([]byte)
					if !ok {
						g.log.Errorf("Can not convert to byte2")
					}
					msgsV[tsubs.Name] = string(bytes.Trim(v, "\t\u0000"))
				} else {
					msgsV[tsubs.Name] = fmt.Sprint(tvalue)
				}
				//log.Println("address:", tsubs.Address, " Value:", val, " original:", tvalue)
			}
			msg := g.createMessageFromValue(subs, msgsV)
			msgs = append(msgs, msg)
			g.subscription[i] = subs
		}

	}

	return msgs, func(ctx context.Context, err error) error {
		return nil // Acknowledgment handling here if needed
	}, nil
}

func (g *ABCommInput) Close(ctx context.Context) error {
	//log.Println("Girish Close()")
	if g.client != nil {
		g.client.Disconnect()
	}
	return nil
}

// createMessageFromValue creates a benthos messages from a given variant and nodeID
// theoretically nodeID can be extracted from variant, but not in all cases (e.g., when subscribing), so it it left to the calling function
func (g *ABCommInput) createMessageFromValue(subscriptionDef subscriptionDef, messageJ map[string]string) *service.Message {
	re := regexp.MustCompile(`[^a-zA-Z0-9_-]`)
	if subscriptionDef.Value == nil {
		g.log.Errorf("Value is nil")
		return nil
	}
	message := service.NewMessage(nil)

	if subscriptionDef.DataType == "string" {
		v, ok := subscriptionDef.Value.([]byte)
		if !ok {
			log.Println("Can not convert to byte2")
		}
		value := string(bytes.Trim(v, "\t\u0000"))
		message.MetaSet("value", value)
	} else {
		message.MetaSet("value", fmt.Sprint(subscriptionDef.Value))
	}

	//message.MetaSet("tag_name", tagName)
	message.MetaSet("name", subscriptionDef.Address)
	message.MetaSet("group", subscriptionDef.Group)
	message.MetaSet("db", subscriptionDef.DB)
	message.MetaSet("historian", subscriptionDef.Historian)
	message.MetaSet("sqlSp", subscriptionDef.SqlSp)
	message.MetaSet("trigger", subscriptionDef.Address)
	newAddress := make(map[string]string)
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
	message.MetaSet("datatype", subscriptionDef.DataType)

	/* if subscriptionDef.DataType == "str" {
		v, ok := subscriptionDef.Value.([]byte)
		if !ok {
			log.Println("Can not convert to byte1 ", subscriptionDef.Value)
		}
		message.MetaSet("value", string(v))
	} else {
		message.MetaSet("value", fmt.Sprintf("%s", subscriptionDef.Value))
	} */

	return message

}

/* func cleanString(str any) string {
	re := regexp.MustCompile("[\a\x00 ]+") //split according to \s, \t, \r, \t and whitespace. Edit this regex for other 'conditions'

	split := re.ReplaceAllLiteralString(str.(string), "")
	return split
} */
