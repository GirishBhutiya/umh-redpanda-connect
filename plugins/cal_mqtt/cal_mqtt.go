package cal_mqtt

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type mqttReader struct {
	url           string // Address of mqtt server.
	clientId      string
	keepAlive     int
	mergeTopics   []mergeTopics
	inputTopics   inputTopic
	qos           uint8
	cleanSession  bool
	oldValue      map[string][]byte
	timeout       time.Duration
	client        mqtt.Client
	msgChan       chan mqtt.Message
	cMut          sync.Mutex
	interruptChan chan struct{}
	log           *service.Logger
}
type mergeTopics struct {
	ID     int
	topics []mergeTopic
}
type inputTopic struct {
	topic       string
	outputTopic string
}
type mergeTopic struct {
	topic    string
	elements []string
	oldValue []byte
}

var calMQTTConfigSpec = service.NewConfigSpec().
	Summary("Read data from provided  MQTT broker and merge it.").
	Description("This input plugin enables Benthos to read data directly from provded mqtt  server " +
		"and merges the messages received on multiple topic into one message").
	Field(service.NewStringField("url").Description("url of mqtt server")).
	Field(service.NewStringField("client_id").Description("client id")).
	Field(service.NewIntField("qos").Description("The level of delivery guarantee to enforce. Has options 0, 1, 2.").Default(1)).
	Field(service.NewIntField("keepalive").Description("Max seconds of inactivity before a keepalive message is sent.").Default(30)).
	Field(service.NewDurationField("timeout").Description("The maximum amount of time to wait in order to establish a connection before the attempt is abandoned.").Default("30s")).
	Field(service.NewBoolField("clean_session").Description("Set whether the connection is non-persistent.").Default(true)).
	Field(service.NewStringListField("tsubscriptions").Description("List of topics to merge")).
	Field(service.NewStringListField("subscriptions").Description("List of topics to merge"))

func newCalMQTTSubInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {

	url, err := conf.FieldString("url")
	if err != nil {
		return nil, err
	}

	clientId, err := conf.FieldString("client_id")
	if err != nil {
		return nil, err
	}

	qos, err := conf.FieldInt("qos")
	if err != nil {
		return nil, err
	}
	keepalive, err := conf.FieldInt("keepalive")
	if err != nil {
		return nil, err
	}
	ctimeout, err := conf.FieldDuration("timeout")
	if err != nil {
		return nil, err
	}
	cleanSession, err := conf.FieldBool("clean_session")
	if err != nil {
		return nil, err
	}

	mergeTopics, err := conf.FieldStringList("tsubscriptions")
	if err != nil {
		return nil, err
	}

	inputTopics, err := conf.FieldStringList("subscriptions")
	if err != nil {
		return nil, err
	}

	mTopics := parseMergeTopics(mergeTopics)
	iTopics := parseInputTopic(inputTopics)

	m := &mqttReader{
		url:           url,
		clientId:      clientId,
		mergeTopics:   mTopics,
		inputTopics:   iTopics,
		qos:           uint8(qos),
		cleanSession:  cleanSession,
		keepAlive:     keepalive,
		timeout:       ctimeout,
		oldValue:      make(map[string][]byte),
		interruptChan: make(chan struct{}),
		log:           mgr.Logger(),
	}

	return service.AutoRetryNacksBatched(m), nil
}

func init() {

	err := service.RegisterBatchInput(
		"mqtttrigger", calMQTTConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			mgr.Logger().Infof("Created & maintained by the BGRI ")
			return newCalMQTTSubInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func (g *mqttReader) Connect(ctx context.Context) error {
	g.cMut.Lock()
	defer g.cMut.Unlock()

	if g.client != nil {
		return nil
	}

	var msgMut sync.Mutex
	msgChan := make(chan mqtt.Message)

	closeMsgChan := func() bool {
		msgMut.Lock()
		chanOpen := msgChan != nil
		if chanOpen {
			close(msgChan)
			msgChan = nil
		}
		msgMut.Unlock()
		return chanOpen
	}
	topics := getAllTopics(g.inputTopics, g.mergeTopics, g.qos)
	//log.Println("All Topics:", topics)
	conf := g.apply(mqtt.NewClientOptions()).
		SetCleanSession(g.cleanSession).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			client.Disconnect(0)
			closeMsgChan()
			g.log.Errorf("Connection lost due to: %v\n", reason)
		}).
		SetOnConnectHandler(func(c mqtt.Client) {

			tok := c.SubscribeMultiple(topics, func(c mqtt.Client, msg mqtt.Message) {
				msgMut.Lock()
				if msgChan != nil {
					select {
					case msgChan <- msg:
					case <-g.interruptChan:
					}
				}
				msgMut.Unlock()
			})
			tok.Wait()
			if err := tok.Error(); err != nil {
				//log.Println("Girish\n", err)
				g.log.Errorf("Failed to subscribe to topics '%v': %v", topics, err)
				g.log.Error("Shutting connection down.")
				closeMsgChan()
			}
		})

	client := mqtt.NewClient(conf)

	tok := client.Connect()
	tok.Wait()
	if err := tok.Error(); err != nil {
		return err
	}

	g.log.Infof("Receiving MQTT messages from topics: %v", topics)

	go func() {
		for {
			select {
			case <-time.After(time.Second):
				if !client.IsConnected() {
					if closeMsgChan() {
						g.log.Error("Connection lost for unknown reasons.")
					}
					return
				}
			case <-g.interruptChan:
				return
			}
		}
	}()

	g.client = client
	g.msgChan = msgChan
	return nil

}
func (g *mqttReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	g.cMut.Lock()
	msgChan := g.msgChan
	g.cMut.Unlock()
	if msgChan == nil {
		return nil, nil, errors.New("not connected to target source or sink")
	}
	msgs := service.MessageBatch{}
	select {
	case msg, open := <-msgChan:
		if !open {
			g.cMut.Lock()
			g.msgChan = nil
			g.client = nil
			g.cMut.Unlock()
			return nil, nil, service.ErrNotConnected
		}
		/* var anyT any
		err := json.Unmarshal(msg.Payload(), &anyT)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal message payload into JSON: %w", err)
		} */
		//log.Println(compareTopic(msg.Topic(), g.inputTopics.topic) && !bytes.Equal(g.oldValue[msg.Topic()], msg.Payload()))
		if compareTopic(msg.Topic(), g.inputTopics.topic) && !bytes.Equal(g.oldValue[msg.Topic()], msg.Payload()) {

			//log.Println("Parsed elements:", getElementsParsed(g.mergeTopics, msg.Topic(), c))
			data, err := getServiceMessage(msg, g.mergeTopics)
			if err != nil {
				return nil, nil, err
			}

			message := service.NewMessage(data)
			message.MetaSet("outputtopic", getOutputTopic(msg.Topic(), g.inputTopics.outputTopic))
			message.MetaSetMut("mqtt_duplicate", msg.Duplicate())
			message.MetaSetMut("mqtt_qos", int(msg.Qos()))
			message.MetaSetMut("mqtt_retained", msg.Retained())
			message.MetaSetMut("mqtt_topic", msg.Topic())
			message.MetaSetMut("mqtt_message_id", int(msg.MessageID()))
			g.oldValue[msg.Topic()] = msg.Payload()
			msgs = append(msgs, message)
			/* return message, func(ctx context.Context, res error) error {
				if res == nil {
					msg.Ack()
				}
				return nil
			}, nil */
		} else {
			for i, v := range g.mergeTopics {
				for j, h := range v.topics {
					if msg.Topic() == h.topic {
						g.mergeTopics[i].topics[j].oldValue = msg.Payload()
					}
				}
			}
		}
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-g.interruptChan:
		return nil, nil, service.ErrEndOfInput
	}
	return msgs, func(ctx context.Context, err error) error {
		return nil // Acknowledgment handling here if needed
	}, nil
}

func (g *mqttReader) Close(ctx context.Context) error {
	g.cMut.Lock()
	defer g.cMut.Unlock()

	if g.client != nil {
		g.client.Disconnect(0)
		g.client = nil
		close(g.interruptChan)
	}
	return nil
}
